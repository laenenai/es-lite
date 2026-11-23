package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/sqlite"
)

// The binary's first tests: they drive run() end to end — env, openBackend,
// dispatch, exit code — against a migrated SQLite file, exactly as the
// audit runs on a box. F13: an empty stream or an empty database is an
// error, never a clean `ok`.

// newTestDB migrates a fresh SQLite database in a temp dir and points the
// CLI's env at it. The binary opens stores without auto-migrate, so the
// schema must exist before run() is called; the returned store lets a
// subtest seed the database first. Close it before calling run — run opens
// its own handle.
func newTestDB(t *testing.T) *sqlite.Store {
	t.Helper()
	dsn := "file:" + filepath.Join(t.TempDir(), "eslite.db")
	t.Setenv("BACKEND", "sqlite")
	t.Setenv("SQLITE_DSN", dsn)
	s, err := sqlite.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// seedStream appends n events to sid through the store itself, so the
// chain in the file was written by the store, not by the test.
func seedStream(t *testing.T, s *sqlite.Store, sid es.StreamID, n int) {
	t.Helper()
	events := make([]es.EventData, n)
	for i := range events {
		events[i] = es.EventData{TypeURL: "thing.v1.Happened", SchemaVersion: 1, Payload: []byte{byte(i + 1)}}
	}
	if _, err := s.Append(context.Background(), es.AppendParams{StreamID: sid, Events: events}); err != nil {
		t.Fatalf("append %d events: %v", n, err)
	}
}

// closeDB closes the seeding handle before run opens its own. Closing
// twice is fine (the cleanup handle no-ops), and every subtest needs it.
func closeDB(t *testing.T, s *sqlite.Store) {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatalf("close sqlite: %v", err)
	}
}

// TestVerifyStreamEmptyStreamIsNotIntact pins the CLI half of review focus
// 2: an empty stream must never read `ok` (F13), and --allow-empty is the
// only way an empty stream exits 0.
func TestVerifyStreamEmptyStreamIsNotIntact(t *testing.T) {
	t.Run("stream on empty db exits 2", func(t *testing.T) {
		s := newTestDB(t)
		closeDB(t, s)
		if code := run([]string{"stream", "thing:a"}); code != exitUsage {
			t.Errorf("stream on a never-written stream: exit %d, want %d", code, exitUsage)
		}
	})

	t.Run("all on empty db exits 2", func(t *testing.T) {
		s := newTestDB(t)
		closeDB(t, s)
		if code := run([]string{"all"}); code != exitUsage {
			t.Errorf("all on a database with zero streams: exit %d, want %d", code, exitUsage)
		}
	})

	t.Run("stream on ok stream exits 0", func(t *testing.T) {
		s := newTestDB(t)
		sid, err := es.NewStreamID("thing", "a")
		if err != nil {
			t.Fatalf("stream id: %v", err)
		}
		seedStream(t, s, sid, 2)
		closeDB(t, s)
		if code := run([]string{"stream", sid.Canonical()}); code != exitIntact {
			t.Errorf("stream on an intact chain: exit %d, want %d", code, exitIntact)
		}
	})

	t.Run("stream --allow-empty on empty exits 0", func(t *testing.T) {
		s := newTestDB(t)
		closeDB(t, s)
		if code := run([]string{"stream", "--allow-empty", "thing:a"}); code != exitIntact {
			t.Errorf("stream --allow-empty on a never-written stream: exit %d, want %d", code, exitIntact)
		}
	})

	t.Run("migrate+verify roundtrip sets exit 1 on a broken chain", func(t *testing.T) {
		s := newTestDB(t)
		sid, err := es.NewStreamID("thing", "a")
		if err != nil {
			t.Fatalf("stream id: %v", err)
		}
		seedStream(t, s, sid, 2)
		// Tamper with v1's payload behind the store's back: the stored hash
		// no longer matches what the row's fields hash to.
		if _, err := s.DB().ExecContext(context.Background(),
			`UPDATE events SET payload = ? WHERE stream_id = ? AND version = 1`,
			[]byte("tampered"), sid.Canonical()); err != nil {
			t.Fatalf("tamper: %v", err)
		}
		closeDB(t, s)
		if code := run([]string{"stream", sid.Canonical()}); code != exitBroken {
			t.Errorf("stream on a tampered chain: exit %d, want %d", code, exitBroken)
		}
	})
}

// captureOutput runs fn with os.Stdout and os.Stderr redirected into
// pipes and returns what each collected. The copiers drain the pipes
// concurrently, so fn can write freely; the originals are restored even
// on failure.
func captureOutput(t *testing.T, fn func()) (stdout, stderr string) {
	t.Helper()
	oldOut, oldErr := os.Stdout, os.Stderr
	rOut, wOut, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe stdout: %v", err)
	}
	rErr, wErr, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe stderr: %v", err)
	}
	os.Stdout, os.Stderr = wOut, wErr
	defer func() {
		os.Stdout, os.Stderr = oldOut, oldErr
		rOut.Close()
		rErr.Close()
	}()
	var outBuf, errBuf syncBuffer
	done := make(chan struct{}, 2)
	go func() { io.Copy(&outBuf, rOut); done <- struct{}{} }()
	go func() { io.Copy(&errBuf, rErr); done <- struct{}{} }()
	fn()
	wOut.Close()
	wErr.Close()
	<-done
	<-done
	return outBuf.String(), errBuf.String()
}

// syncBuffer is a bytes.Buffer safe for the concurrent copiers above.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestVerifyOrderOnSQLite pins the CLI's real sqlite order path (Task 8
// replaced the transitional stub the earlier test pinned): `order`
// deep-verifies the seeded batches and exits 0; `all` verifies the
// streams first and then the log order, exiting 0; and a repositioned
// event — caught by neither chain nor heads — reports BROKEN at the
// tampered batch's from_position and exits 1.
func TestVerifyOrderOnSQLite(t *testing.T) {
	t.Run("order on a seeded db exits 0", func(t *testing.T) {
		s := newTestDB(t)
		sid, err := es.NewStreamID("thing", "a")
		if err != nil {
			t.Fatalf("stream id: %v", err)
		}
		seedStream(t, s, sid, 2)
		closeDB(t, s)
		var code int
		stdout, _ := captureOutput(t, func() { code = run([]string{"order"}) })
		if code != exitIntact {
			t.Errorf("order on sqlite: exit %d, want %d", code, exitIntact)
		}
		if !strings.Contains(stdout, "log order: 1 batches verified") {
			t.Errorf("order on sqlite: stdout %q, want the one seeded batch verified", stdout)
		}
	})

	t.Run("all verifies streams then order and exits 0", func(t *testing.T) {
		s := newTestDB(t)
		sid, err := es.NewStreamID("thing", "a")
		if err != nil {
			t.Fatalf("stream id: %v", err)
		}
		seedStream(t, s, sid, 2)
		closeDB(t, s)
		var code int
		stdout, _ := captureOutput(t, func() { code = run([]string{"all"}) })
		if code != exitIntact {
			t.Errorf("all on sqlite: exit %d, want %d", code, exitIntact)
		}
		if !strings.Contains(stdout, "ok      thing:a head=v2") {
			t.Errorf("all on sqlite: stdout %q, want the stream verified ok before the order stage", stdout)
		}
		if !strings.Contains(stdout, "es-verify: 1 streams checked") {
			t.Errorf("all on sqlite: stdout %q, want the streams-checked summary", stdout)
		}
		if !strings.Contains(stdout, "log order: 1 batches verified") {
			t.Errorf("all on sqlite: stdout %q, want the order stage's ok after the streams", stdout)
		}
	})

	t.Run("order reports a repositioned event and exits 1", func(t *testing.T) {
		s := newTestDB(t)
		sid, err := es.NewStreamID("thing", "a")
		if err != nil {
			t.Fatalf("stream id: %v", err)
		}
		// Two single-event batches: their log_order rows attest one
		// position each, so swapping the events' positions is the F11
		// tamper — invisible to the chain and the heads.
		var positions []uint64
		for i := 0; i < 2; i++ {
			res, err := s.Append(context.Background(), es.AppendParams{
				StreamID: sid, ExpectedVersion: uint64(i),
				Events: []es.EventData{seedEvent(i)},
			})
			if err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
			positions = append(positions, res.Envelopes[0].GlobalPosition)
		}
		swapPositions(t, s, sid, positions[0], positions[1])
		closeDB(t, s)
		var code int
		stdout, _ := captureOutput(t, func() { code = run([]string{"order"}) })
		if code != exitBroken {
			t.Errorf("order on a repositioned log: exit %d, want %d", code, exitBroken)
		}
		if !strings.Contains(stdout, fmt.Sprintf("BROKEN  log order at batch from_position %d", positions[0])) {
			t.Errorf("order on a repositioned log: stdout %q, want BROKEN at from_position %d", stdout, positions[0])
		}
	})
}

// seedEvent is one typed event for the order tests above.
func seedEvent(i int) es.EventData {
	return es.EventData{TypeURL: "thing.v1.Happened", SchemaVersion: 1, Payload: []byte{byte(i + 1)}}
}

// swapPositions exchanges the two events' global positions behind the
// store's back. SQLite enforces INTEGER PRIMARY KEY (rowid) uniqueness
// per row inside one UPDATE, so the swap parks both rows on the
// negative mirror of their positions first (positions are always
// positive) and then moves each into the other's old spot.
func swapPositions(t *testing.T, s *sqlite.Store, sid es.StreamID, p1, p2 uint64) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.DB().ExecContext(ctx,
		`UPDATE events SET global_position = -global_position WHERE stream_id = ? AND global_position IN (?, ?)`,
		sid.Canonical(), p1, p2); err != nil {
		t.Fatalf("park positions %d/%d: %v", p1, p2, err)
	}
	res, err := s.DB().ExecContext(ctx,
		`UPDATE events SET global_position = CASE global_position WHEN ? THEN ? WHEN ? THEN ? END
		  WHERE stream_id = ? AND global_position IN (?, ?)`,
		-int64(p1), int64(p2), -int64(p2), int64(p1), sid.Canonical(), -int64(p1), -int64(p2))
	if err != nil {
		t.Fatalf("swap positions %d/%d: %v", p1, p2, err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 2 {
		t.Fatalf("swap positions %d/%d: rows %d (err %v), want 2", p1, p2, n, err)
	}
}

// TestAllExitCodePrecedence pins the exit-code merge of `es-verify
// all` (final review I3): a broken verdict (exit 1) must dominate a
// usage/EMPTY verdict (exit 2). The order stage's exit code used to
// overwrite the streams stage's unconditionally, so a tampered stream
// on a store with zero attested batches — exactly a pre-0006 legacy
// store — exited 2 (order EMPTY), masking the tamper as a usage error.
func TestAllExitCodePrecedence(t *testing.T) {
	t.Run("tampered stream over a store with no attested batches exits 1", func(t *testing.T) {
		s := newTestDB(t)
		sid, err := es.NewStreamID("thing", "a")
		if err != nil {
			t.Fatal(err)
		}
		seedStream(t, s, sid, 2)
		// Tamper v1's payload behind the store's back: the stored hash
		// no longer matches what the row's fields hash to.
		if _, err := s.DB().ExecContext(context.Background(),
			`UPDATE events SET payload = ? WHERE stream_id = ? AND version = 1`,
			[]byte("tampered"), sid.Canonical()); err != nil {
			t.Fatalf("tamper: %v", err)
		}
		// A pre-0006-shaped store: no attested batches at all.
		if _, err := s.DB().ExecContext(context.Background(), `DELETE FROM log_order`); err != nil {
			t.Fatalf("strip log_order rows: %v", err)
		}
		closeDB(t, s)
		if code := run([]string{"all"}); code != exitBroken {
			t.Errorf("all over a tampered stream and zero attested batches: exit %d, want %d — broken must dominate the order stage's EMPTY verdict", code, exitBroken)
		}
	})

	t.Run("intact streams over a store with no attested batches exits 2", func(t *testing.T) {
		s := newTestDB(t)
		sid, err := es.NewStreamID("thing", "a")
		if err != nil {
			t.Fatal(err)
		}
		seedStream(t, s, sid, 2)
		if _, err := s.DB().ExecContext(context.Background(), `DELETE FROM log_order`); err != nil {
			t.Fatalf("strip log_order rows: %v", err)
		}
		closeDB(t, s)
		// The events-only shape: an intact chain but no order witness.
		// `all` still includes the order stage, which is EMPTY (exit 2);
		// --chains-only is the restore path that stops after the chains.
		if code := run([]string{"all"}); code != exitUsage {
			t.Errorf("all over intact streams and zero attested batches: exit %d, want %d — EMPTY certifies nothing (F13)", code, exitUsage)
		}
		var code int
		stdout, _ := captureOutput(t, func() { code = run([]string{"all", "--chains-only"}) })
		if code != exitIntact {
			t.Errorf("all --chains-only over an intact chain with no log_order: exit %d, want %d", code, exitIntact)
		}
		if !strings.Contains(stdout, "ok      thing:a head=v2") ||
			!strings.Contains(stdout, "es-verify: 1 streams checked") {
			t.Errorf("all --chains-only: stdout %q, want the stream chain verified", stdout)
		}
		if strings.Contains(stdout, "log order") {
			t.Errorf("all --chains-only: stdout %q, must not run the order stage", stdout)
		}
	})
}

// seedClaim appends one event carrying one Claim constraint, so the store
// also records the ops on that event — the row the claims commands operate on.
func seedClaim(t *testing.T, s *sqlite.Store, sid es.StreamID, scope, value string) {
	t.Helper()
	if _, err := s.Append(context.Background(), es.AppendParams{
		StreamID:    sid,
		Events:      []es.EventData{seedEvent(0)},
		Constraints: []es.ConstraintOp{es.Claim(scope, value)},
	}); err != nil {
		t.Fatalf("append with claim: %v", err)
	}
}

// TestClaimsCommandEmpty pins the claims oracle's empty case: a fresh
// database has no ops and no claims, so the verify reports OK with zero op
// records and exits 0 — the index and its log agree trivially.
func TestClaimsCommandEmpty(t *testing.T) {
	s := newTestDB(t)
	closeDB(t, s)
	var code int
	stdout, _ := captureOutput(t, func() { code = run([]string{"claims"}) })
	if code != exitIntact {
		t.Errorf("claims on an empty db: exit %d, want %d", code, exitIntact)
	}
	if !strings.Contains(stdout, "claims OK: 0 op records") {
		t.Errorf("claims on an empty db: stdout %q, want the zero-op OK", stdout)
	}
}

// TestClaimsCommandRebuildAndVerify pins the rebuild/verify pair: a seeded
// claim rebuilds to exactly one op record and one claim, verifies OK, and
// rebuilding again is idempotent — the same event log replays to the same
// index and still verifies.
func TestClaimsCommandRebuildAndVerify(t *testing.T) {
	s := newTestDB(t)
	sid, err := es.NewStreamID("thing", "a")
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	seedClaim(t, s, sid, "thing.slug", "a")
	closeDB(t, s)

	for i := 0; i < 2; i++ {
		var code int
		stdout, _ := captureOutput(t, func() { code = run([]string{"claims", "--rebuild"}) })
		if code != exitIntact {
			t.Fatalf("claims --rebuild (run %d): exit %d, want %d", i+1, code, exitIntact)
		}
		if !strings.Contains(stdout, "claims rebuilt: 1 op records, 1 claims") {
			t.Errorf("claims --rebuild (run %d): stdout %q, want the seeded claim rebuilt", i+1, stdout)
		}

		stdout, _ = captureOutput(t, func() { code = run([]string{"claims"}) })
		if code != exitIntact {
			t.Fatalf("claims after rebuild (run %d): exit %d, want %d", i+1, code, exitIntact)
		}
		if !strings.Contains(stdout, "claims OK: 1 op records") {
			t.Errorf("claims after rebuild (run %d): stdout %q, want the rebuilt index verified", i+1, stdout)
		}
	}
}

// fakeClaims is a minimal es.ClaimsMaintainer for the command tests: they
// drive VerifyClaims, and RebuildClaims exists to satisfy the interface.
type fakeClaims struct {
	rep es.ClaimsVerifyReport
	err error
}

func (f fakeClaims) RebuildClaims(context.Context) (es.ClaimsRebuildReport, error) {
	return es.ClaimsRebuildReport{}, nil
}

func (f fakeClaims) VerifyClaims(context.Context) (es.ClaimsVerifyReport, error) {
	return f.rep, f.err
}

// TestClaimsCommandDivergenceDominatesError pins the claims verify exit
// precedence across workspaces: a divergence already confirmed on one
// workspace is a tamper, and a later workspace's read/connection error must
// not mask it as "nothing audited" (exit 2). Both orders must exit 1, per
// mergeExit's broken-dominates rule, and the divergence names its workspace.
func TestClaimsCommandDivergenceDominatesError(t *testing.T) {
	diverged := fakeClaims{rep: es.ClaimsVerifyReport{Ops: 1, FirstDivergence: "scope=thing.slug value=61 stream=thing:a"}}
	failed := fakeClaims{err: errors.New("read failed")}
	for _, tc := range []struct {
		name          string
		first, second fakeClaims
		divergedWS    string
	}{
		{"divergence then error", diverged, failed, "a"},
		{"error then divergence", failed, diverged, "b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &backend{
				claims: func(ws string) es.ClaimsMaintainer {
					if ws == "a" {
						return tc.first
					}
					return tc.second
				},
				claimWorkspaces: func(context.Context) ([]string, error) {
					return []string{"a", "b"}, nil
				},
				multiWS: true,
			}
			var code int
			stdout, stderr := captureOutput(t, func() { code = cmdClaims(context.Background(), b, nil) })
			if code != exitBroken {
				t.Errorf("claims across a divergence and an error: exit %d, want %d — the divergence must dominate", code, exitBroken)
			}
			if want := "DIVERGED: " + tc.divergedWS + " " + diverged.rep.FirstDivergence; !strings.Contains(stdout, want) {
				t.Errorf("claims across a divergence and an error: stdout %q, want %q — the divergence must name its workspace", stdout, want)
			}
			if !strings.Contains(stderr, "read failed") {
				t.Errorf("claims across a divergence and an error: stderr %q, want the workspace error", stderr)
			}
		})
	}
}

// TestMergeExitCombinations pins the merge rule itself, over every
// verdict pair `es-verify all` can fold: broken (1) dominates usage or
// EMPTY (2) in both orders, intact (0) never overrides anything, and
// like verdicts stay themselves.
func TestMergeExitCombinations(t *testing.T) {
	for _, tc := range []struct {
		name          string
		code, c, want int
	}{
		{"broken+broken stays 1", exitBroken, exitBroken, exitBroken},
		{"intact+empty stays 2", exitIntact, exitUsage, exitUsage},
		{"broken+empty becomes 1", exitBroken, exitUsage, exitBroken},
		{"empty+broken becomes 1", exitUsage, exitBroken, exitBroken},
		{"intact+broken becomes 1", exitIntact, exitBroken, exitBroken},
		{"broken+intact stays 1", exitBroken, exitIntact, exitBroken},
		{"intact+intact stays 0", exitIntact, exitIntact, exitIntact},
		{"empty+intact stays 2", exitUsage, exitIntact, exitUsage},
		{"empty+empty stays 2", exitUsage, exitUsage, exitUsage},
	} {
		if got := mergeExit(tc.code, tc.c); got != tc.want {
			t.Errorf("%s: mergeExit(%d, %d) = %d, want %d", tc.name, tc.code, tc.c, got, tc.want)
		}
	}
}

func seedTaggedStream(t *testing.T, s *sqlite.Store, sid es.StreamID, n int) {
	t.Helper()
	events := make([]es.EventData, n)
	for i := range events {
		events[i] = es.EventData{
			TypeURL: "thing.v1.Happened",
			Payload: []byte{byte(i + 1)},
			Tags:    map[string]string{"flowdoc.block": fmt.Sprintf("b%d", i+1)},
		}
	}
	if _, err := s.Append(context.Background(), es.AppendParams{
		StreamID: sid,
		Events:   events,
		Tags:     map[string]string{"flowdoc.space": "s1"},
	}); err != nil {
		t.Fatalf("append tagged events: %v", err)
	}
}

func TestTagsCommandEmpty(t *testing.T) {
	s := newTestDB(t)
	sid, _ := es.NewStreamID("thing", "untagged")
	seedStream(t, s, sid, 2)
	closeDB(t, s)

	if code := run([]string{"tags"}); code != exitUsage {
		t.Errorf("tags on an untagged log: exit %d, want %d (EMPTY)", code, exitUsage)
	}
	// all must not fail on an untagged log: tags are optional metadata.
	if code := run([]string{"all"}); code != exitIntact {
		t.Errorf("all on an untagged log: exit %d, want %d", code, exitIntact)
	}
}

func TestTagsCommandRebuildAndVerify(t *testing.T) {
	ctx := context.Background()
	s := newTestDB(t)
	sid, _ := es.NewStreamID("thing", "tagged")
	seedTaggedStream(t, s, sid, 2)
	if _, err := s.DB().ExecContext(ctx,
		`DELETE FROM event_tags WHERE rowid IN (SELECT rowid FROM event_tags WHERE tag_key = 'flowdoc.block' LIMIT 1)`); err != nil {
		t.Fatalf("delete a tag row: %v", err)
	}
	closeDB(t, s)

	if code := run([]string{"tags", "--rebuild", "default"}); code != exitIntact {
		t.Fatalf("tags --rebuild: exit %d, want %d", code, exitIntact)
	}
	if code := run([]string{"tags", "default"}); code != exitIntact {
		t.Fatalf("tags <workspace> after rebuild: exit %d, want %d", code, exitIntact)
	}

	handle, err := sqlite.Open(ctx, os.Getenv("SQLITE_DSN"))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := handle.DB().ExecContext(ctx,
		`DELETE FROM event_tags WHERE rowid IN (SELECT rowid FROM event_tags WHERE tag_key = 'flowdoc.block' LIMIT 1)`); err != nil {
		handle.Close()
		t.Fatalf("tamper tag row: %v", err)
	}
	handle.Close()

	if code := run([]string{"tags", "default"}); code != exitBroken {
		t.Errorf("tags <workspace> after divergence: exit %d, want %d", code, exitBroken)
	}
}

func TestTagsCommandAttestation(t *testing.T) {
	ctx := context.Background()
	s := newTestDB(t)
	sid, _ := es.NewStreamID("thing", "tagged-attest")
	seedTaggedStream(t, s, sid, 2)
	closeDB(t, s)

	if code := run([]string{"tags"}); code != exitIntact {
		t.Fatalf("tags attestation: exit %d, want %d", code, exitIntact)
	}

	handle, err := sqlite.Open(ctx, os.Getenv("SQLITE_DSN"))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := handle.DB().ExecContext(ctx, `UPDATE log_order SET tags_hash = zeroblob(32)`); err != nil {
		handle.Close()
		t.Fatalf("tamper tags_hash: %v", err)
	}
	handle.Close()

	if code := run([]string{"tags"}); code != exitBroken {
		t.Errorf("tags attestation after tamper: exit %d, want %d", code, exitBroken)
	}
	if code := run([]string{"all"}); code != exitBroken {
		t.Errorf("all after tags_hash tamper: exit %d, want %d", code, exitBroken)
	}
	if code := run([]string{"all", "--chains-only"}); code != exitIntact {
		t.Errorf("all --chains-only after tags_hash tamper: exit %d, want %d", code, exitIntact)
	}
}

func TestTagsCommandUsage(t *testing.T) {
	s := newTestDB(t)
	closeDB(t, s)
	if code := run([]string{"tags", "--rebuild", "a", "b"}); code != exitUsage {
		t.Errorf("tags with two workspaces: exit %d, want %d", code, exitUsage)
	}
	if code := run([]string{"tags", "--nope"}); code != exitUsage {
		t.Errorf("tags with an unknown flag: exit %d, want %d", code, exitUsage)
	}
}
