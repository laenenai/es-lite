// Command es-verify audits the tamper-evident hash chain (ADR 0029) and
// manages anchors. It needs no keys: it recomputes every hash from what the
// database returns. Exit status is the audit result (0 intact, 1 broken,
// 2 usage/connection error). An empty stream or an empty database is an
// error, not an intact result: it certifies nothing (review F13).
//
// Usage:
//
//	es-verify stream [--allow-empty] [<workspace>] <type:id>
//	  verify one stream (workspace required on Postgres; a never-written
//	  stream is an error unless --allow-empty)
//	es-verify all [--strict]
//	  verify every stream (--strict: every row must be chained), then the
//	  global log order (each log_order batch hash, recomputed from the
//	  events)
//	es-verify anchor [--upto N]
//	  take an anchor and print it as JSON (export it off-box)
//	es-verify anchor --check <anchor.json>
//	  recompute an exported anchor and compare
//	es-verify anchor list
//	  list recorded anchors
//	es-verify order [--upto N]
//	  deep-verify the log order: recompute every log_order batch hash
//	  from the events at or below the bound (0 = the log head)
//
// Config (env), as es-migrate: BACKEND (postgres|sqlite), PG_DSN, SQLITE_DSN.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/postgres"
	"github.com/laenenai/es-lite/sqlite"
)

const (
	exitIntact = 0
	exitBroken = 1
	exitUsage  = 2
)

// backend is what the commands need from either store.
type backend struct {
	es.Anchorer
	scope        func(workspace string) es.Store
	chainedSince func(context.Context) (time.Time, error)
	// verifyOrder is the deep log-order verification, present only on
	// backends that implement it (Postgres). Nil means the backend
	// cannot deep-verify order yet: the order stage then surfaces the
	// backend's own OrderRows error and exits 2 — never a silent skip,
	// never a vacuous pass (sqlite's log-order attestation lands in a
	// later task).
	verifyOrder func(context.Context, uint64) (*postgres.OrderVerifyReport, error)
	multiWS     bool
	close       func()
}

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		usage()
		return exitUsage
	}
	ctx := context.Background()
	b, err := openBackend(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "es-verify: %v\n", err)
		return exitUsage
	}
	defer b.close()

	switch args[0] {
	case "stream":
		return cmdStream(ctx, b, args[1:])
	case "all":
		return cmdAll(ctx, b, args[1:])
	case "anchor":
		return cmdAnchor(ctx, b, args[1:])
	case "order":
		return cmdOrder(ctx, b, args[1:])
	default:
		usage()
		return exitUsage
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  es-verify stream [--allow-empty] [<workspace>] <type:id>
  es-verify all [--strict]
  es-verify anchor [--upto N] | --check <anchor.json> | list
  es-verify order [--upto N]`)
}

func cmdStream(ctx context.Context, b *backend, args []string) int {
	fs := flag.NewFlagSet("stream", flag.ContinueOnError)
	allowEmpty := fs.Bool("allow-empty", false, "report an empty (never-written) stream as intact instead of an error")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	var ws, canonical string
	rest := fs.Args()
	switch {
	case b.multiWS && len(rest) == 2:
		ws, canonical = rest[0], rest[1]
	case !b.multiWS && len(rest) >= 1:
		canonical = rest[len(rest)-1]
	default:
		usage()
		return exitUsage
	}
	sid, err := es.ParseCanonical(canonical)
	if err != nil {
		fmt.Fprintf(os.Stderr, "es-verify: %v\n", err)
		return exitUsage
	}
	since, err := b.chainedSince(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "es-verify: %v\n", err)
		return exitUsage
	}
	return verifyOne(ctx, b, ws, sid, es.VerifyOptions{ChainedSince: since}, *allowEmpty)
}

func cmdAll(ctx context.Context, b *backend, args []string) int {
	fs := flag.NewFlagSet("all", flag.ContinueOnError)
	strict := fs.Bool("strict", false, "require every row to be chained (ChainedFrom == 1)")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	since, err := b.chainedSince(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "es-verify: %v\n", err)
		return exitUsage
	}
	refs, err := b.ListStreams(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "es-verify: list streams: %v\n", err)
		return exitUsage
	}
	if len(refs) == 0 {
		// An audit over zero streams certified nothing while reading as a
		// clean exit (F13) — usually the wrong database or an unmigrated
		// one, not an intact log.
		fmt.Fprintln(os.Stderr, "es-verify: no streams found")
		return exitUsage
	}
	code := exitIntact
	for _, ref := range refs {
		if c := verifyOne(ctx, b, ref.Workspace, ref.StreamID, es.VerifyOptions{ChainedSince: since, Strict: *strict}, false); c != exitIntact {
			code = c
		}
	}
	fmt.Printf("es-verify: %d streams checked\n", len(refs))
	// The order stage runs after the stream chains: first what each
	// stream contains, then how the batches landed in the global log.
	// A backend that cannot deep-verify order yet says so loudly here
	// (exit 2, its own OrderRows error) instead of passing vacuously.
	if c := cmdOrder(ctx, b, nil); c != exitIntact {
		code = c
	}
	return code
}

func verifyOne(ctx context.Context, b *backend, ws string, sid es.StreamID, opts es.VerifyOptions, allowEmpty bool) int {
	rep, err := es.VerifyStreamWith(ctx, b.scope(ws), sid, opts)
	label := sid.Canonical()
	if ws != "" {
		label = ws + " " + label
	}
	switch {
	case err != nil && errors.Is(err, es.ErrChainBroken):
		fmt.Printf("BROKEN  %s at v%d: %v\n", label, rep.BrokenAt, err)
		return exitBroken
	case err != nil:
		fmt.Fprintf(os.Stderr, "es-verify: %s: %v\n", label, err)
		return exitUsage
	case rep.Head == 0 && !allowEmpty:
		// A stream that verifies to head 0 attests nothing: it was never
		// written (Task 2's sentinel, tolerated by VerifyStreamWith) or
		// every row is gone. Reading it as `ok` certified an empty audit
		// (F13).
		fmt.Printf("EMPTY   %s\n", label)
		return exitUsage
	default:
		fmt.Printf("ok      %s head=v%d chained_from=%d head_hash=%s\n", label, rep.Head, rep.ChainedFrom, hex.EncodeToString(rep.HeadHash))
		return exitIntact
	}
}

// cmdOrder runs the deep log-order verification (design D1): for every
// log_order row at or below the bound, recompute the row's batch hash
// from the events and compare — the audit that catches a repositioned
// event, which every stream chain and the heads root survive. Printing
// shares verifyOne's shape; a mismatch is exit 1 with the offending
// row's from_position, and an anchor-free log with zero attested rows
// is EMPTY (exit 2, F13: it certifies nothing). A backend without the
// deep verify (sqlite until its log-order attestation lands) surfaces
// its own OrderRows error and exits 2 — no silent skip, no vacuous
// pass.
func cmdOrder(ctx context.Context, b *backend, args []string) int {
	fs := flag.NewFlagSet("order", flag.ContinueOnError)
	upto := fs.Uint64("upto", 0, "verify log order at or below this global_position (0 = log head)")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if fs.NArg() > 0 {
		usage()
		return exitUsage
	}
	if b.verifyOrder == nil {
		_, err := b.OrderRows(ctx, *upto)
		if err == nil {
			err = errors.New("this backend cannot deep-verify log order yet")
		}
		fmt.Fprintf(os.Stderr, "es-verify: %v\n", err)
		return exitUsage
	}
	rep, err := b.verifyOrder(ctx, *upto)
	if err != nil {
		fmt.Fprintf(os.Stderr, "es-verify: %v\n", err)
		return exitUsage
	}
	switch {
	case !rep.OK:
		m := rep.Mismatch
		fmt.Printf("BROKEN  log order at batch from_position %d: batch_hash %s, recomputed %s\n",
			m.From, hex.EncodeToString(m.ExpectedBatchHash), hex.EncodeToString(m.ActualBatchHash))
		return exitBroken
	case rep.RowsChecked == 0:
		// Zero attested rows certifies nothing about order (F13):
		// everything at or below the bound predates log-order
		// attestation.
		fmt.Println("EMPTY   log order: no attested batches")
		return exitUsage
	default:
		fmt.Printf("ok      log order: %d batches verified\n", rep.RowsChecked)
		return exitIntact
	}
}

func cmdAnchor(ctx context.Context, b *backend, args []string) int {
	if len(args) > 0 && args[0] == "list" {
		anchors, err := b.ListAnchors(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "es-verify: %v\n", err)
			return exitUsage
		}
		enc := json.NewEncoder(os.Stdout)
		for _, a := range anchors {
			_ = enc.Encode(a)
		}
		return exitIntact
	}
	fs := flag.NewFlagSet("anchor", flag.ContinueOnError)
	upto := fs.Uint64("upto", 0, "anchor every stream at or below this global_position (0 = log head)")
	check := fs.String("check", "", "path to an exported anchor JSON to recompute and compare")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if *check != "" {
		raw, err := os.ReadFile(*check)
		if err != nil {
			fmt.Fprintf(os.Stderr, "es-verify: %v\n", err)
			return exitUsage
		}
		var a es.Anchor
		if err := json.Unmarshal(raw, &a); err != nil {
			fmt.Fprintf(os.Stderr, "es-verify: parse anchor: %v\n", err)
			return exitUsage
		}
		if err := b.CheckAnchor(ctx, a); err != nil {
			if errors.Is(err, es.ErrAnchorMismatch) {
				fmt.Printf("MISMATCH %v\n", err)
				return exitBroken
			}
			fmt.Fprintf(os.Stderr, "es-verify: %v\n", err)
			return exitUsage
		}
		fmt.Printf("ok      anchor %s at global_position %d matches\n", a.ID, a.UptoGlobalPosition)
		return exitIntact
	}
	a, err := b.Anchor(ctx, *upto)
	if err != nil {
		fmt.Fprintf(os.Stderr, "es-verify: anchor: %v\n", err)
		return exitUsage
	}
	out, _ := json.Marshal(a)
	fmt.Println(string(out))
	return exitIntact
}

func openBackend(ctx context.Context) (*backend, error) {
	kind := os.Getenv("BACKEND")
	if kind == "" {
		kind = "postgres"
	}
	switch kind {
	case "postgres":
		dsn := os.Getenv("PG_DSN")
		if dsn == "" {
			return nil, errors.New("PG_DSN is required for BACKEND=postgres")
		}
		store, err := postgres.Open(ctx, dsn, postgres.WithoutAutoMigrate())
		if err != nil {
			return nil, fmt.Errorf("open postgres: %w", err)
		}
		return &backend{
			Anchorer:     store,
			scope:        store.Workspace,
			chainedSince: store.ChainedSince,
			verifyOrder:  store.VerifyOrder,
			multiWS:      true,
			close:        store.Close,
		}, nil
	case "sqlite":
		dsn := os.Getenv("SQLITE_DSN")
		if dsn == "" {
			// The default stays — single-box setups rely on it — but it is
			// the wrong database more often than not: say so loudly (F36).
			fmt.Fprintln(os.Stderr, "es-verify: using default SQLITE_DSN file:eslite.db in the working directory")
			dsn = "file:eslite.db"
		}
		store, err := sqlite.Open(ctx, dsn, sqlite.WithoutAutoMigrate())
		if err != nil {
			return nil, fmt.Errorf("open sqlite: %w", err)
		}
		return &backend{
			Anchorer:     store,
			scope:        func(string) es.Store { return store },
			chainedSince: store.ChainedSince,
			close:        func() { _ = store.Close() },
		}, nil
	default:
		return nil, fmt.Errorf("unknown BACKEND %q (want postgres|sqlite)", kind)
	}
}
