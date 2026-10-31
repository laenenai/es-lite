// Command es-verify audits the tamper-evident hash chain (ADR 0029) and
// manages anchors. It needs no keys: it recomputes every hash from what the
// database returns. Exit status is the audit result (0 intact, 1 broken,
// 2 usage/connection error).
//
// Usage:
//
//	es-verify stream [<workspace>] <type:id>     verify one stream (workspace required on Postgres)
//	es-verify all [--strict]                      verify every stream (--strict: every row must be chained)
//	es-verify anchor [--upto N]                   take an anchor and print it as JSON (export it off-box)
//	es-verify anchor --check <anchor.json>        recompute an exported anchor and compare
//	es-verify anchor list                         list recorded anchors
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
	multiWS      bool
	close        func()
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
	default:
		usage()
		return exitUsage
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  es-verify stream [<workspace>] <type:id>
  es-verify all [--strict]
  es-verify anchor [--upto N] | --check <anchor.json> | list`)
}

func cmdStream(ctx context.Context, b *backend, args []string) int {
	var ws, canonical string
	switch {
	case b.multiWS && len(args) == 2:
		ws, canonical = args[0], args[1]
	case !b.multiWS && len(args) >= 1:
		canonical = args[len(args)-1]
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
	return verifyOne(ctx, b, ws, sid, es.VerifyOptions{ChainedSince: since})
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
	code := exitIntact
	for _, ref := range refs {
		if c := verifyOne(ctx, b, ref.Workspace, ref.StreamID, es.VerifyOptions{ChainedSince: since, Strict: *strict}); c != exitIntact {
			code = c
		}
	}
	fmt.Printf("es-verify: %d streams checked\n", len(refs))
	return code
}

func verifyOne(ctx context.Context, b *backend, ws string, sid es.StreamID, opts es.VerifyOptions) int {
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
	default:
		fmt.Printf("ok      %s head=v%d chained_from=%d head_hash=%s\n", label, rep.Head, rep.ChainedFrom, hex.EncodeToString(rep.HeadHash))
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
			multiWS:      true,
			close:        store.Close,
		}, nil
	case "sqlite":
		dsn := os.Getenv("SQLITE_DSN")
		if dsn == "" {
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
