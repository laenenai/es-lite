package postgres_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/postgres"
)

// The command-dedup adapter (design D6, finding F6) over migration
// 0010's command_dedup table: the es.Deduplicator the generated handler
// flow (Task 33) will replay stored responses through.
// postgres.NewDedupStore binds to a Workspace, so every operation runs
// with app.workspace_id pinned and the table's RLS — the workspace arm
// only, no bypass arm — confines it to that workspace's records.

// TestDedupRoundTrip pins the storage half of the Deduplicator contract:
// Save stores the completed command's record, Lookup returns it
// verbatim, and a key that was never saved is a miss — (zero, false,
// nil) — not an error and not a made-up record.
func TestDedupRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	d := postgres.NewDedupStore(s.WorkspaceStore("ws_dedup_rt"))

	key := es.DedupKey{
		StreamID:  "counter:rt-1",
		CommandID: "0192f3a0-0000-7000-8000-0000000000c1",
	}

	// The never-saved key: a miss with the zero record and no error.
	rec, found, err := d.Lookup(ctx, key)
	if err != nil {
		t.Fatalf("lookup before save: %v", err)
	}
	if found {
		t.Fatal("lookup before save: found=true, want false — the key was never saved")
	}
	if rec.Fingerprint != "" || rec.Response != nil {
		t.Fatalf("lookup before save: record = %+v, want the zero record", rec)
	}

	saved := es.DedupRecord{
		Fingerprint: "fp-round-trip",
		Response:    []byte(`{"count":1}`),
	}
	if err := d.Save(ctx, key, saved); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, found, err := d.Lookup(ctx, key)
	if err != nil {
		t.Fatalf("lookup after save: %v", err)
	}
	if !found {
		t.Fatal("lookup after save: found=false, want true")
	}
	if got.Fingerprint != saved.Fingerprint {
		t.Fatalf("fingerprint = %q, want %q", got.Fingerprint, saved.Fingerprint)
	}
	if string(got.Response) != string(saved.Response) {
		t.Fatalf("response = %q, want %q", got.Response, saved.Response)
	}
}

// TestDedupSaveConflictIsNotError pins the conflict-silent half of the
// contract (es.Deduplicator, migration 0010's header): the first
// completed response wins forever, so a Save whose key already holds a
// record stores nothing and returns nil — the conflict is NOT an error —
// and the re-Lookup the contract prescribes returns the FIRST record.
func TestDedupSaveConflictIsNotError(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	d := postgres.NewDedupStore(s.WorkspaceStore("ws_dedup_conflict"))

	key := es.DedupKey{
		StreamID:  "counter:conflict-1",
		CommandID: "0192f3a0-0000-7000-8000-0000000000c3",
	}
	first := es.DedupRecord{
		Fingerprint: "fp-first",
		Response:    []byte(`{"result":"first"}`),
	}
	second := es.DedupRecord{
		Fingerprint: "fp-second",
		Response:    []byte(`{"result":"second"}`),
	}

	if err := d.Save(ctx, key, first); err != nil {
		t.Fatalf("first save: %v", err)
	}
	// The conflicting Save: ON CONFLICT DO NOTHING swallows the
	// primary-key hit and returns nil.
	if err := d.Save(ctx, key, second); err != nil {
		t.Fatalf("second save: %v — a Save that hits the conflict is not an error by contract", err)
	}
	// The re-Lookup must return the FIRST record: the silent conflict
	// must not have overwritten it.
	got, found, err := d.Lookup(ctx, key)
	if err != nil || !found {
		t.Fatalf("re-lookup: found=%v err=%v", found, err)
	}
	if got.Fingerprint != first.Fingerprint || string(got.Response) != string(first.Response) {
		t.Fatalf("re-lookup = %+v, want the FIRST record %+v", got, first)
	}
}

// TestDedupWorkspaceIsolation pins command_dedup's workspace privacy:
// the table carries ONLY the workspace RLS arm — no bypass arm, unlike
// events — so a dedup record is private to the workspace whose command
// wrote it, invisible to every other workspace and even to the relay
// role under app.bypass_rls (the property 0010's header documents).
//
// Behaviorally, through the adapters: two real workspaces save the SAME
// key with different records and each reads back only its own. Then as
// a role the policies bind (es_rls_probe, the feed tables' probe
// pattern): with no workspace_id filter in the statement the policy
// alone shows a pinned workspace exactly its own row — and with
// es_relay membership plus app.bypass_rls on, the bypass that reads
// events across workspaces reads NOTHING from command_dedup.
func TestDedupWorkspaceIsolation(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ensureRLSProbeRole(t, ctx, s)

	const (
		wsA = "ws_dedup_isol_a"
		wsB = "ws_dedup_isol_b"
	)
	dA := postgres.NewDedupStore(s.WorkspaceStore(wsA))
	dB := postgres.NewDedupStore(s.WorkspaceStore(wsB))

	// One seeded event in wsA: the bypass probe below must be able to
	// read events through events' bypass arm, or the command_dedup half
	// would prove nothing (a probe that can read nowhere is not a
	// refused probe).
	sid, err := es.NewStreamID("counter", "isol")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Workspace(wsA).Append(ctx, es.AppendParams{
		StreamID: sid,
		Events: []es.EventData{{
			TypeURL:       "counter.v1.Seeded",
			SchemaVersion: 1,
			Payload:       []byte("seed"),
		}},
	}); err != nil {
		t.Fatalf("seed event in %s: %v", wsA, err)
	}

	// The same key in both workspaces, with different records: if the
	// table keyed rows without workspace scoping, B's Save would hit
	// A's row — silently, per the conflict contract — and B's Lookup
	// would return A's record instead of its own.
	key := es.DedupKey{
		StreamID:  sid.Canonical(),
		CommandID: "0192f3a0-0000-7000-8000-0000000000c2",
	}
	recA := es.DedupRecord{Fingerprint: "fp-a", Response: []byte(`{"from":"a"}`)}
	recB := es.DedupRecord{Fingerprint: "fp-b", Response: []byte(`{"from":"b"}`)}
	if err := dA.Save(ctx, key, recA); err != nil {
		t.Fatalf("save under %s: %v", wsA, err)
	}
	if err := dB.Save(ctx, key, recB); err != nil {
		t.Fatalf("save under %s: %v", wsB, err)
	}

	// Each workspace reads back its own record under the same key.
	gotA, found, err := dA.Lookup(ctx, key)
	if err != nil || !found {
		t.Fatalf("lookup in %s: found=%v err=%v", wsA, found, err)
	}
	if gotA.Fingerprint != recA.Fingerprint || string(gotA.Response) != string(recA.Response) {
		t.Fatalf("lookup in %s = %+v, want its own record %+v", wsA, gotA, recA)
	}
	gotB, found, err := dB.Lookup(ctx, key)
	if err != nil || !found {
		t.Fatalf("lookup in %s: found=%v err=%v", wsB, found, err)
	}
	if gotB.Fingerprint != recB.Fingerprint || string(gotB.Response) != string(recB.Response) {
		t.Fatalf("lookup in %s = %+v, want its own record %+v", wsB, gotB, recB)
	}

	// The probe needs SELECT and INSERT on command_dedup to reach the
	// policy at all — the counts read through it, the cross-workspace
	// insert below must be refused BY it, not by a permission wall;
	// ensureRLSProbeRole grants the feed tables and events only. The
	// grant is this test's own and is revoked on cleanup so the role is
	// left exactly as the feed tests expect it.
	if _, err := s.Pool().Exec(ctx, `GRANT SELECT, INSERT ON command_dedup TO `+rlsProbeRole); err != nil {
		t.Fatalf("grant %s on command_dedup: %v", rlsProbeRole, err)
	}
	t.Cleanup(func() {
		if _, err := s.Pool().Exec(context.Background(), `REVOKE SELECT, INSERT ON command_dedup FROM `+rlsProbeRole); err != nil {
			t.Errorf("revoke %s on command_dedup: %v", rlsProbeRole, err)
		}
	})

	// The policy alone decides: pinned to one workspace, with no
	// workspace_id filter anywhere in the statement, the probe sees
	// exactly that workspace's row — 1, not the 2 the table holds.
	for _, ws := range []string{wsA, wsB} {
		err := probePinned(t, ctx, s, ws, func(tx pgx.Tx) error {
			var n int
			if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM command_dedup`).Scan(&n); err != nil {
				return err
			}
			if n != 1 {
				return fmt.Errorf("count under %s = %d, want 1 — the policy alone must show the pinned workspace only its own row", ws, n)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("probe pinned to %s: %v", ws, err)
		}
	}

	// WITH CHECK: a cross-workspace insert is refused at the policy —
	// pinned to wsB, writing wsA's id must fail with the row-level
	// security violation, and there is no bypass arm to escape through,
	// because the policy never references one.
	err = probePinned(t, ctx, s, wsB, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO command_dedup (workspace_id, stream_id, command_id, fingerprint, response)
			 VALUES ($1, $2, $3, $4, $5)`,
			wsA, "counter:foreign", "0192f3a0-0000-7000-8000-0000000000c4", "fp-foreign", []byte("x"),
		)
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "row-level security") {
		t.Fatalf("cross-workspace insert under %s's GUC: got %v, want a row-level security policy violation", wsB, err)
	}

	// No bypass arm, behaviorally: the same probe, granted es_relay and
	// holding app.bypass_rls on with NO workspace pinned, reads events
	// across workspaces — the arm exists there — but command_dedup
	// returns nothing: even the relay role cannot read a foreign
	// workspace's dedup records.
	if _, err := s.Pool().Exec(ctx, `GRANT `+relayRole+` TO `+rlsProbeRole); err != nil {
		t.Fatalf("grant %s to %s: %v", relayRole, rlsProbeRole, err)
	}
	t.Cleanup(func() {
		if _, err := s.Pool().Exec(context.Background(), `REVOKE `+relayRole+` FROM `+rlsProbeRole); err != nil {
			t.Errorf("revoke %s from %s: %v", relayRole, rlsProbeRole, err)
		}
	})
	dedupN, eventsN, err := bypassProbeCounts(ctx, s.Pool())
	if err != nil {
		t.Fatalf("bypass probe: %v", err)
	}
	if dedupN != 0 {
		t.Fatalf("bypass probe: command_dedup count = %d, want 0 — the policy has no bypass arm", dedupN)
	}
	if eventsN == 0 {
		t.Fatal("bypass probe: events count = 0, want > 0 — the same probe must read events through events' bypass arm, or the command_dedup half proves nothing")
	}

	// The schema pin, in the feed tables' shape (not file greps, which
	// false-positive on comments): exactly one policy, keying on
	// app.workspace_id with NO app.bypass_rls reference in either arm —
	// the workspace arm is the whole policy — and both RLS flags set,
	// FORCE keeping the table owner subject to it.
	rows, err := s.Pool().Query(ctx, `
		SELECT policyname, qual, with_check FROM pg_policies
		WHERE schemaname = 'public' AND tablename = 'command_dedup'`)
	if err != nil {
		t.Fatalf("read policies for command_dedup: %v", err)
	}
	var policies []struct{ name, qual, withCheck string }
	for rows.Next() {
		var name, qual, withCheck string
		if err := rows.Scan(&name, &qual, &withCheck); err != nil {
			rows.Close()
			t.Fatalf("scan policy for command_dedup: %v", err)
		}
		policies = append(policies, struct{ name, qual, withCheck string }{name, qual, withCheck})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("policies rows for command_dedup: %v", err)
	}
	if len(policies) != 1 {
		t.Fatalf("command_dedup: %d policies, want exactly 1", len(policies))
	}
	p := policies[0]
	for _, def := range []string{p.qual, p.withCheck} {
		if strings.Contains(def, "app.bypass_rls") {
			t.Fatalf("command_dedup: policy %q references app.bypass_rls (%q) — the policy must have no bypass arm", p.name, def)
		}
		if !strings.Contains(def, "app.workspace_id") {
			t.Fatalf("command_dedup: policy %q does not key on app.workspace_id (%q)", p.name, def)
		}
	}
	var enabled, forced bool
	if err := s.Pool().QueryRow(ctx,
		`SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE oid = $1::regclass`,
		"public.command_dedup",
	).Scan(&enabled, &forced); err != nil {
		t.Fatalf("read pg_class flags for command_dedup: %v", err)
	}
	if !enabled || !forced {
		t.Fatalf("command_dedup: RLS enabled=%v forced=%v, want both true (FORCE keeps the table owner subject)", enabled, forced)
	}
}

// bypassProbeCounts runs as the RLS probe role holding es_relay
// membership and app.bypass_rls on, with no workspace pinned, and
// returns the raw row counts the policies alone admit on command_dedup
// and events. The SET LOCAL ROLE and the is_local GUC die with the
// transaction.
func bypassProbeCounts(ctx context.Context, pool *pgxpool.Pool) (dedup, events int, err error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+rlsProbeRole); err != nil {
		return 0, 0, err
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.bypass_rls', 'on', true)`); err != nil {
		return 0, 0, err
	}
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM command_dedup`).Scan(&dedup); err != nil {
		return 0, 0, err
	}
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM events`).Scan(&events); err != nil {
		return 0, 0, err
	}
	return dedup, events, nil
}
