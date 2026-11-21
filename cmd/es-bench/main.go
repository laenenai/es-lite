// Command es-bench seeds and measures a Postgres es-lite store for the scale
// benchmark recorded in docs/backlog/2026-11-21_events-scale-benchmark.md.
//
// It is a benchmark tool, not a production command. Two facts about the seed:
// rows are inserted in bulk (not through the append transaction, so the
// append-latency numbers are measured separately and honestly) and the seeded
// rows carry no hash chain (NULL prev_hash/hash), because the benchmark is
// about the claims replay path, not chain verification. Run `measure` against
// a seeded store; the append phase then writes real, chained streams through
// Workspace.Append.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/postgres"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "seed":
		err = runSeed(ctx, os.Args[2:])
	case "measure":
		err = runMeasure(ctx, os.Args[2:])
	case "reset":
		err = runReset(ctx, os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "es-bench: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  es-bench seed    -dsn DSN [-events N] [-op-every N] [-events-per-stream N] [-workspaces N]
  es-bench measure -dsn DSN [-workspace W] [-appends N] [-batch N] [-claim-every N] [-json]
  es-bench reset   -dsn DSN

seed writes synthetic events with constraint ops every N rows; it does not use
the append path and does not build a hash chain. measure runs real appends,
then RebuildClaims and VerifyClaims, and prints the footprint.`)
}

// seedSQL inserts one position range. $11 is the range's first global
// position, used only to keep stream/version numbering local to the
// workspace. Every opEvery-th row carries the encoded ops blob, with a unique
// 32-byte value so the claims index can be rebuilt without collisions.
const seedSQL = `
INSERT INTO events (
  workspace_id, global_position, event_id, stream_type, stream_id, version,
  type_url, schema_version, occurred_at, recorded_at,
  correlation_id, causation_id, command_id,
  actor_type, actor_id, actor_principal, payload, constraint_ops
)
SELECT
  $1, g, gen_random_uuid()::text, 'bench',
  'bench:' || (((g - $11) / $2)::text),
  ((g - $11) % $2) + 1,
  'type.googleapis.com/bench.v1.Event', 1, $3::timestamptz, $3::timestamptz,
  $4, $4, $4, 'bench', 'bench', 'bench', $5::bytea,
  CASE WHEN g % $6::bigint = 0
       THEN overlay($7::bytea PLACING decode(md5(g::text) || md5((g + 1)::text), 'hex') FROM $8::int FOR 32)
       ELSE NULL END
FROM generate_series($9::bigint, $10::bigint) AS g`

// opsValueBytes is the width of the placeholder claim value in the seed blob.
// The encoder appends the value last, so the replacement starts there.
const opsValueBytes = 32

func runSeed(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	dsn := fs.String("dsn", defaultDSN(), "Postgres DSN")
	workspaceCount := fs.Int("workspaces", 1, "number of workspaces to spread the events over")
	events := fs.Int64("events", 10_000_000, "total events to seed")
	opEvery := fs.Int64("op-every", 100, "one constraint-bearing row every N events (>= 1)")
	eventsPerStream := fs.Int64("events-per-stream", 100, "events per synthetic stream")
	chunk := fs.Int64("chunk", 1_000_000, "rows per INSERT transaction")
	reset := fs.Bool("reset", true, "truncate events, log_order and unique_claims first")
	vacuum := fs.Bool("vacuum", true, "VACUUM (ANALYZE) events after seeding")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *events <= 0 || *opEvery < 1 || *eventsPerStream < 1 || *chunk < 1 || *workspaceCount < 1 {
		return fmt.Errorf("events, op-every, events-per-stream, chunk and workspaces must be positive")
	}

	store, err := postgres.Open(ctx, *dsn)
	if err != nil {
		return err
	}
	defer store.Close()
	pool := store.Pool()

	if *reset {
		if _, err := pool.Exec(ctx, `TRUNCATE events, log_order, unique_claims RESTART IDENTITY`); err != nil {
			return fmt.Errorf("truncate: %w", err)
		}
	}

	base := es.EncodeConstraintOps([]es.ConstraintOp{
		es.Claim("bench.claim", strings.Repeat("\x00", opsValueBytes)),
	})
	valueStart := len(base) - opsValueBytes + 1 // SQL is 1-based
	payload := bytes32(0xab)
	nilID := uuid.Nil.String()
	now := time.Now().UTC()

	started := time.Now()
	for w := 1; w <= *workspaceCount; w++ {
		first := int64(w-1)*(*events)/int64(*workspaceCount) + 1
		last := int64(w) * (*events) / int64(*workspaceCount)
		if w == *workspaceCount {
			last = *events
		}
		if first > last {
			continue
		}
		ws := fmt.Sprintf("bench-ws-%d", w)
		for lo := first; lo <= last; lo += *chunk {
			hi := lo + *chunk - 1
			if hi > last {
				hi = last
			}
			chunkStart := time.Now()
			if _, err := pool.Exec(ctx, seedSQL,
				ws, *eventsPerStream, now, nilID, payload, *opEvery,
				base, valueStart, lo, hi, first,
			); err != nil {
				return fmt.Errorf("seed %s %d-%d: %w", ws, lo, hi, err)
			}
			fmt.Printf("seeded %s %d-%d in %.1fs\n", ws, lo, hi, time.Since(chunkStart).Seconds())
		}
	}

	if _, err := pool.Exec(ctx, `SELECT setval('events_global_position_seq', $1, true)`, *events); err != nil {
		return fmt.Errorf("setval: %w", err)
	}
	if *vacuum {
		fmt.Println("vacuum analyze events ...")
		if _, err := pool.Exec(ctx, `VACUUM (ANALYZE) events`); err != nil {
			return fmt.Errorf("vacuum: %w", err)
		}
	}

	var rows, opRows int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE constraint_ops IS NOT NULL) FROM events
	`).Scan(&rows, &opRows); err != nil {
		return fmt.Errorf("count seeded rows: %w", err)
	}
	fmt.Printf("seeded %d events (%d op-bearing, op-every %d) over %d workspace(s) in %.1fs\n",
		rows, opRows, *opEvery, *workspaceCount, time.Since(started).Seconds())
	return nil
}

func runMeasure(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("measure", flag.ExitOnError)
	dsn := fs.String("dsn", defaultDSN(), "Postgres DSN")
	workspace := fs.String("workspace", "bench-ws-1", "workspace to measure")
	appends := fs.Int("appends", 2_000, "real appends to time")
	batch := fs.Int("batch", 10, "events per timed append")
	claimEvery := fs.Int("claim-every", 1, "every Nth append carries a claim (0 disables)")
	skipAppends := fs.Bool("skip-appends", false, "skip the append-latency phase")
	skipRebuild := fs.Bool("skip-rebuild", false, "skip RebuildClaims")
	skipVerify := fs.Bool("skip-verify", false, "skip VerifyClaims")
	asJSON := fs.Bool("json", false, "print the result as JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *appends < 0 || *batch < 1 || *claimEvery < 0 {
		return fmt.Errorf("appends must be >= 0, batch >= 1, claim-every >= 0")
	}

	store, err := postgres.Open(ctx, *dsn)
	if err != nil {
		return err
	}
	defer store.Close()
	w := store.WorkspaceStore(*workspace)

	res := measureResult{
		Workspace: *workspace,
		Appends:   appendResult{Batch: *batch, ClaimEvery: *claimEvery},
	}
	if res.Footprint, err = footprint(ctx, store.Pool(), *workspace); err != nil {
		return err
	}

	if !*skipAppends && *appends > 0 {
		runID := time.Now().UnixNano()
		payload := bytes32(0xcd)
		durations := make([]time.Duration, 0, *appends)
		for i := 0; i < *appends; i++ {
			sid, err := es.NewStreamID("bench", fmt.Sprintf("r%d-%d", runID, i))
			if err != nil {
				return err
			}
			events := make([]es.EventData, *batch)
			for j := range events {
				events[j] = es.EventData{
					TypeURL:       "type.googleapis.com/bench.v1.Event",
					SchemaVersion: 1,
					Payload:       payload,
				}
			}
			p := es.AppendParams{
				StreamID:      sid,
				Events:        events,
				CommandID:     uuid.New(),
				CorrelationID: uuid.New(),
				Actor:         es.Actor{Type: "bench", ID: "bench"},
				OccurredAt:    time.Now().UTC(),
			}
			if *claimEvery > 0 && i%*claimEvery == 0 {
				p.Constraints = []es.ConstraintOp{
					es.Claim("bench.claim", fmt.Sprintf("claim-%d-%d", runID, i)),
				}
			}
			start := time.Now()
			if _, err := w.Append(ctx, p); err != nil {
				return fmt.Errorf("append %d: %w", i, err)
			}
			durations = append(durations, time.Since(start))
		}
		res.Appends = summarizeAppends(durations, *batch, *claimEvery)
	}

	if !*skipRebuild {
		start := time.Now()
		rep, err := w.RebuildClaims(ctx)
		if err != nil {
			return fmt.Errorf("rebuild claims: %w", err)
		}
		res.Rebuild = rebuildResult{
			Seconds: time.Since(start).Seconds(), Ops: rep.Ops, Claims: rep.Claims,
		}
	}

	if !*skipVerify {
		start := time.Now()
		rep, err := w.VerifyClaims(ctx)
		if err != nil {
			return fmt.Errorf("verify claims: %w", err)
		}
		res.Verify = verifyResult{
			Seconds: time.Since(start).Seconds(), OK: rep.OK,
			Ops: rep.Ops, Claims: rep.Claims, FirstDivergence: rep.FirstDivergence,
		}
		if !rep.OK {
			return fmt.Errorf("verify claims: DIVERGED at %s", rep.FirstDivergence)
		}
	}

	if res.FootprintAfter, err = footprint(ctx, store.Pool(), *workspace); err != nil {
		return err
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	printMeasure(res)
	return nil
}

func runReset(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("reset", flag.ExitOnError)
	dsn := fs.String("dsn", defaultDSN(), "Postgres DSN")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := postgres.Open(ctx, *dsn)
	if err != nil {
		return err
	}
	defer store.Close()
	if _, err := store.Pool().Exec(ctx,
		`TRUNCATE events, log_order, unique_claims RESTART IDENTITY`); err != nil {
		return err
	}
	if _, err := store.Pool().Exec(ctx, `SELECT setval('events_global_position_seq', 1, false)`); err != nil {
		return err
	}
	fmt.Println("reset: events, log_order and unique_claims truncated; position sequence reset")
	return nil
}

type measureResult struct {
	Workspace      string            `json:"workspace"`
	Footprint      map[string]string `json:"footprint_before"`
	Appends        appendResult      `json:"appends"`
	Rebuild        rebuildResult     `json:"rebuild"`
	Verify         verifyResult      `json:"verify"`
	FootprintAfter map[string]string `json:"footprint_after"`
}

type appendResult struct {
	Count      int     `json:"count"`
	Batch      int     `json:"batch"`
	ClaimEvery int     `json:"claim_every"`
	P50Ms      float64 `json:"p50_ms"`
	P99Ms      float64 `json:"p99_ms"`
	MeanMs     float64 `json:"mean_ms"`
	MaxMs      float64 `json:"max_ms"`
}

type rebuildResult struct {
	Seconds float64 `json:"seconds"`
	Ops     int64   `json:"ops"`
	Claims  int64   `json:"claims"`
}

type verifyResult struct {
	Seconds         float64 `json:"seconds"`
	OK              bool    `json:"ok"`
	Ops             int64   `json:"ops"`
	Claims          int64   `json:"claims"`
	FirstDivergence string  `json:"first_divergence,omitempty"`
}

func summarizeAppends(durations []time.Duration, batch, claimEvery int) appendResult {
	r := appendResult{Count: len(durations), Batch: batch, ClaimEvery: claimEvery}
	if len(durations) == 0 {
		return r
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	var total time.Duration
	for _, d := range durations {
		total += d
	}
	r.MeanMs = ms(total / time.Duration(len(durations)))
	r.MaxMs = ms(durations[len(durations)-1])
	r.P50Ms = ms(durations[len(durations)*50/100])
	p99 := len(durations) * 99 / 100
	if p99 >= len(durations) {
		p99 = len(durations) - 1
	}
	r.P99Ms = ms(durations[p99])
	return r
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func footprint(ctx context.Context, pool *pgxpool.Pool, ws string) (map[string]string, error) {
	var events, opRows, claims, logOrder int64
	if err := pool.QueryRow(ctx, `
		SELECT
		  (SELECT count(*) FROM events WHERE workspace_id = $1),
		  (SELECT count(*) FROM events WHERE workspace_id = $1 AND constraint_ops IS NOT NULL),
		  (SELECT count(*) FROM unique_claims WHERE workspace_id = $1),
		  (SELECT count(*) FROM log_order WHERE workspace_id = $1)
	`, ws).Scan(&events, &opRows, &claims, &logOrder); err != nil {
		return nil, fmt.Errorf("footprint counts: %w", err)
	}

	var total, heap, indexes string
	if err := pool.QueryRow(ctx, `
		SELECT
		  pg_size_pretty(COALESCE(sum(pg_total_relation_size(c.oid)), 0)),
		  pg_size_pretty(COALESCE(sum(pg_relation_size(c.oid)), 0)),
		  pg_size_pretty(COALESCE(sum(pg_indexes_size(c.oid)), 0))
		FROM pg_class c
		JOIN pg_inherits i ON i.inhrelid = c.oid
		WHERE i.inhparent = 'events'::regclass
	`).Scan(&total, &heap, &indexes); err != nil {
		return nil, fmt.Errorf("footprint sizes: %w", err)
	}

	var opsIndex string
	if err := pool.QueryRow(ctx, `
		SELECT pg_size_pretty(COALESCE(sum(pg_relation_size(i.indexrelid)), 0))
		FROM pg_index i
		WHERE pg_get_indexdef(i.indexrelid) LIKE '%constraint_ops IS NOT NULL%'
		  AND i.indrelid IN (
		    SELECT c.oid FROM pg_class c
		    JOIN pg_inherits g ON g.inhrelid = c.oid
		    WHERE g.inhparent = 'events'::regclass)
	`).Scan(&opsIndex); err != nil {
		return nil, fmt.Errorf("footprint ops index: %w", err)
	}

	return map[string]string{
		"events":    strconv.FormatInt(events, 10),
		"op_rows":   strconv.FormatInt(opRows, 10),
		"claims":    strconv.FormatInt(claims, 10),
		"log_order": strconv.FormatInt(logOrder, 10),
		"total":     total,
		"heap":      heap,
		"indexes":   indexes,
		"ops_index": opsIndex,
	}, nil
}

func defaultDSN() string {
	if dsn := os.Getenv("PG_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://postgres:pw@127.0.0.1:5435/postgres?sslmode=disable"
}

func bytes32(b byte) []byte {
	out := make([]byte, 32)
	for i := range out {
		out[i] = b
	}
	return out
}

func printMeasure(r measureResult) {
	fmt.Printf("workspace %s\n", r.Workspace)
	fmt.Printf("  footprint: %s\n", footprintLine(r.Footprint))
	if r.Appends.Count > 0 {
		fmt.Printf("  appends: n=%d batch=%d claim-every=%d p50=%.1fms p99=%.1fms mean=%.1fms max=%.1fms\n",
			r.Appends.Count, r.Appends.Batch, r.Appends.ClaimEvery,
			r.Appends.P50Ms, r.Appends.P99Ms, r.Appends.MeanMs, r.Appends.MaxMs)
	}
	if r.Rebuild.Seconds > 0 {
		fmt.Printf("  rebuild: %.2fs ops=%d claims=%d\n", r.Rebuild.Seconds, r.Rebuild.Ops, r.Rebuild.Claims)
	}
	if r.Verify.Seconds > 0 {
		fmt.Printf("  verify: %.2fs ok=%t ops=%d claims=%d\n",
			r.Verify.Seconds, r.Verify.OK, r.Verify.Ops, r.Verify.Claims)
	}
	fmt.Printf("  footprint after: %s\n", footprintLine(r.FootprintAfter))
}

func footprintLine(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+m[k])
	}
	return strings.Join(parts, " ")
}
