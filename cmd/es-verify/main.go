// Command es-verify audits the tamper-evident hash chain (ADR 0029) and
// manages anchors. It needs no keys: it recomputes every hash from what the
// database returns. Exit status is the audit result (0 intact, 1 broken,
// 2 error or refusal — a usage/connection problem, or a claims index the log
// cannot license or reconstruct). An empty stream or an empty database is an
// error, not an intact result: it certifies nothing (review F13).
//
// Usage:
//
//	es-verify stream [--allow-empty] [<workspace>] <type:id>
//	  verify one stream (workspace required on Postgres; a never-written
//	  stream is an error unless --allow-empty)
//	es-verify all [--strict] [--chains-only]
//	  verify every stream (--strict: every row must be chained), then the
//	  global log order (each log_order batch hash, recomputed from the
//	  events). --chains-only stops after the streams: the events-only
//	  restore path, where no log_order was loaded to attest the order
//	es-verify anchor [--upto N]
//	  take an anchor and print it as JSON (export it off-box)
//	es-verify anchor --check <anchor.json>
//	  recompute an exported anchor and compare
//	es-verify anchor list
//	  list recorded anchors
//	es-verify order [--upto N]
//	  deep-verify the log order: recompute every log_order batch hash
//	  from the events at or below the bound (0 = the log head)
//	es-verify claims [--rebuild] [<workspace>]
//	  verify the unique-claims index against the constraint ops on the
//	  event log; --rebuild replays the log over the live index
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
	// verifyOrder is the deep log-order verification. Both in-repo
	// backends provide it; a backend that cannot deep-verify order (nil
	// here) is surfaced loudly by the order stage — its own OrderRows
	// error, exit 2 — never a silent skip, never a vacuous pass.
	verifyOrder func(context.Context, uint64) (*es.OrderVerifyReport, error)
	// claims is the per-workspace claims index maintainer (ADR 0043).
	claims func(workspace string) es.ClaimsMaintainer
	// claimWorkspaces enumerates the workspaces with claim state. Claims
	// are a projection of the events (ADR 0043), so the events arm finds
	// every healthy workspace; the implementation also unions
	// unique_claims so a workspace with claims and no events is discovered
	// and refused, not skipped.
	claimWorkspaces func(context.Context) ([]string, error)
	multiWS         bool
	close           func()
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
	case "claims":
		return cmdClaims(ctx, b, args[1:])
	default:
		usage()
		return exitUsage
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  es-verify stream [--allow-empty] [<workspace>] <type:id>
  es-verify all [--strict] [--chains-only]
  es-verify anchor [--upto N] | --check <anchor.json> | list
  es-verify order [--upto N]
  es-verify claims [--rebuild] [<workspace>]`)
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
	chainsOnly := fs.Bool("chains-only", false, "verify the stream chains only; skip the log-order stage (an events-only restore has no log_order)")
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
		code = mergeExit(code, verifyOne(ctx, b, ref.Workspace, ref.StreamID, es.VerifyOptions{ChainedSince: since, Strict: *strict}, false))
	}
	fmt.Printf("es-verify: %d streams checked\n", len(refs))
	if *chainsOnly {
		// The events-only restore path: log_order was not loaded, so the
		// order stage has no witness to check and would fail EMPTY by
		// design (exit 2). The stream chains are the whole audit here.
		return code
	}
	// The order stage runs after the stream chains: first what each
	// stream contains, then how the batches landed in the global log.
	// A backend that cannot deep-verify order says so loudly here
	// (exit 2, its own OrderRows error) instead of passing vacuously.
	// The merge keeps a broken stream verdict dominant: a tamper must
	// not be masked by the order stage's EMPTY (a pre-0006 store with
	// zero attested batches) or usage verdict.
	return mergeExit(code, cmdOrder(ctx, b, nil))
}

// mergeExit folds one stage's verdict into the running exit code of
// `es-verify all`: broken dominates — a tamper must never be masked by
// a later usage or EMPTY verdict — and intact never overrides anything.
// Every combination: broken+broken 1, intact+empty 2, broken+empty 1,
// empty+broken 1, intact+broken 1, broken+intact 1, empty+empty 2,
// intact+intact 0.
func mergeExit(code, c int) int {
	if c == exitBroken || code == exitIntact {
		return c
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
// deep verify surfaces its own OrderRows error and exits 2 — no silent
// skip, no vacuous pass.
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

// cmdClaims maintains and audits the unique-claims index against the
// constraint ops recorded on the event log (ADR 0043). The default is the
// oracle: rebuild into a temporary index and compare, without touching the
// live index. --rebuild replays the log over the live index. An inconsistent
// state — live claims with no ops in the event log — is refused with
// es.ErrClaimsWithoutDecisions, and that refusal, like connection errors,
// exits 2: nothing was audited.
func cmdClaims(ctx context.Context, b *backend, args []string) int {
	fs := flag.NewFlagSet("claims", flag.ContinueOnError)
	rebuild := fs.Bool("rebuild", false, "replace the live claims index by replaying the constraint ops on the event log")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	rest := fs.Args()
	if len(rest) > 1 {
		usage()
		return exitUsage
	}
	workspaces, err := claimWorkspaces(ctx, b, rest)
	if err != nil {
		fmt.Fprintf(os.Stderr, "es-verify: %v\n", err)
		return exitUsage
	}

	if *rebuild {
		var ops, claims int64
		for _, ws := range workspaces {
			rep, err := b.claims(ws).RebuildClaims(ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "es-verify: %s%v\n", wsPrefix(ws), err)
				return exitUsage
			}
			ops += rep.Ops
			claims += rep.Claims
		}
		fmt.Printf("claims rebuilt: %d op records, %d claims\n", ops, claims)
		return exitIntact
	}

	code := exitIntact
	var ops int64
	for _, ws := range workspaces {
		rep, err := b.claims(ws).VerifyClaims(ctx)
		if err != nil {
			// Keep auditing the other workspaces and fold the verdict
			// through mergeExit: a divergence already confirmed on an
			// earlier workspace is a tamper, and a later read/connection
			// error must not mask it as "nothing audited" (exit 2).
			fmt.Fprintf(os.Stderr, "es-verify: %s%v\n", wsPrefix(ws), err)
			code = mergeExit(code, exitUsage)
			continue
		}
		ops += rep.Ops
		if !rep.OK {
			// Name the workspace like verifyOne does: in a multi-workspace
			// audit the divergence is worthless to an operator who cannot
			// tell which workspace to rebuild.
			fmt.Printf("DIVERGED: %s%s\n", wsLabel(ws), rep.FirstDivergence)
			code = mergeExit(code, exitBroken)
		}
	}
	if code == exitIntact {
		fmt.Printf("claims OK: %d op records\n", ops)
	}
	return code
}

// claimWorkspaces resolves the workspaces a claims command runs over: an
// explicit argument scopes the run to one; otherwise the backend enumerates
// every workspace with claim state. In a healthy store the event log's
// workspace ids are complete, because a constraint-bearing append always
// carries an event; the backend's query also unions unique_claims so a
// workspace with claims and no op-bearing events is discovered and refused
// rather than silently skipped (ADR 0043 §2.2).
func claimWorkspaces(ctx context.Context, b *backend, args []string) ([]string, error) {
	if len(args) == 1 {
		return []string{args[0]}, nil
	}
	if b.claimWorkspaces == nil {
		return nil, errors.New("this backend cannot enumerate claim workspaces")
	}
	workspaces, err := b.claimWorkspaces(ctx)
	if err != nil {
		return nil, fmt.Errorf("list claim workspaces: %w", err)
	}
	return workspaces, nil
}

// wsLabel prefixes a per-workspace line in a multi-workspace run, in
// verifyOne's label style; SQLite's single empty workspace stays unlabeled.
func wsLabel(ws string) string {
	if ws == "" {
		return ""
	}
	return ws + " "
}

// wsPrefix labels a per-workspace error; SQLite's single empty workspace
// stays unlabeled.
func wsPrefix(ws string) string {
	if ws == "" {
		return ""
	}
	return ws + ": "
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
			Anchorer:        store,
			scope:           store.Workspace,
			chainedSince:    store.ChainedSince,
			verifyOrder:     store.VerifyOrder,
			claims:          func(ws string) es.ClaimsMaintainer { return store.WorkspaceStore(ws) },
			claimWorkspaces: store.ClaimWorkspaces,
			multiWS:         true,
			close:           store.Close,
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
			verifyOrder:  store.VerifyOrder,
			claims:       func(string) es.ClaimsMaintainer { return store },
			claimWorkspaces: func(context.Context) ([]string, error) {
				return []string{""}, nil
			},
			close: func() { _ = store.Close() },
		}, nil
	default:
		return nil, fmt.Errorf("unknown BACKEND %q (want postgres|sqlite)", kind)
	}
}
