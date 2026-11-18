package sqlite_test

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/sqlite"
)

// The delegated actor (ADR 0041) on the SQLite backend: the three
// nullable via_* columns migration 0010 adds, the write mapping, the
// strict read reconstruction and the partial via index. Via is an audit
// fact, not telemetry — unlike the span context (0009) a read that
// finds a half-recorded delegation must fail, and the append path
// refuses one before anything is written. SQLite has no database-side
// CHECK (ADR 0041 §3.3: parity would mean another events rebuild), so
// unlike the Postgres suite there is no raw-partial-insert case — the
// writer's validation is the only guard, and these tests pin it.

// viaTypeURL is this file's event type.
const viaTypeURL = "delegation.v1.E"

// appendWithActor appends one viaTypeURL event to sid at the given
// expected version, carrying actor as the appending actor.
func appendWithActor(t testing.TB, ctx context.Context, s *sqlite.Store, sid es.StreamID, version uint64, actor es.Actor) es.Envelope {
	t.Helper()
	res, err := s.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: version,
		Actor:           actor,
		Events:          []es.EventData{{TypeURL: viaTypeURL, SchemaVersion: 1, Payload: []byte("p")}},
	})
	if err != nil {
		t.Fatalf("append at v%d: %v", version+1, err)
	}
	return res.Envelopes[0]
}

// newViaStream returns a stream id unique to the calling test.
func newViaStream(t *testing.T, name string) es.StreamID {
	t.Helper()
	sid, err := es.NewStreamID("delegation", name)
	if err != nil {
		t.Fatal(err)
	}
	return sid
}

// TestDelegatedActorRoundTrip pins the write and read mapping: an
// append carrying a delegated actor comes back with the same actor —
// accountable subject and Via both — on the stream read and the
// global-order read, and the envelope read back still verifies against
// its chain hash, which attests the Via (ADR 0041 §3.2): a store that
// dropped or altered the delegation on the way through would break the
// hash here, not just the field.
func TestDelegatedActorRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := newViaStream(t, "via-rt")
	actor := es.Actor{Type: "user", ID: "alice", Via: es.ActorRef{Type: "service", ID: "agent-runner"}}

	env := appendWithActor(t, ctx, s, sid, 0, actor)
	if env.Actor != actor {
		t.Fatalf("append result carries %+v, want %+v", env.Actor, actor)
	}

	stream, err := s.ReadStream(ctx, sid, 0, 0)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if len(stream) != 1 {
		t.Fatalf("stream has %d events, want 1", len(stream))
	}
	if stream[0].Actor != actor {
		t.Fatalf("ReadStream carries %+v, want %+v", stream[0].Actor, actor)
	}
	if h := es.EnvelopeHash(stream[0]); !bytes.Equal(h[:], stream[0].Hash) {
		t.Fatal("ReadStream envelope fails its chain hash — the persisted Via does not match what was attested")
	}

	all, err := s.ReadAll(ctx, 0, 100)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("log has %d events, want 1", len(all))
	}
	if all[0].Actor != actor {
		t.Fatalf("ReadAll carries %+v, want %+v", all[0].Actor, actor)
	}
}

// TestDelegatedActorAbsentReadsZero pins the absent shape: an append
// with a plain actor — no delegation — reads back with a zero Via and
// the accountable actor unchanged, on both reads.
func TestDelegatedActorAbsentReadsZero(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := newViaStream(t, "via-absent")
	actor := es.Actor{Type: "service", ID: "billing"}

	appendWithActor(t, ctx, s, sid, 0, actor)

	for name, envs := range map[string][]es.Envelope{
		"ReadStream": readStream(t, ctx, s, sid),
		"ReadAll":    readAll(t, ctx, s),
	} {
		if len(envs) != 1 {
			t.Fatalf("%s returned %d events, want 1", name, len(envs))
		}
		if !envs[0].Actor.Via.IsZero() {
			t.Fatalf("%s: Via = %+v, want the zero ActorRef", name, envs[0].Actor.Via)
		}
		if envs[0].Actor != actor {
			t.Fatalf("%s: actor = %+v, want %+v with no Via", name, envs[0].Actor, actor)
		}
	}
}

func readStream(t *testing.T, ctx context.Context, s *sqlite.Store, sid es.StreamID) []es.Envelope {
	t.Helper()
	envs, err := s.ReadStream(ctx, sid, 0, 0)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	return envs
}

func readAll(t *testing.T, ctx context.Context, s *sqlite.Store) []es.Envelope {
	t.Helper()
	envs, err := s.ReadAll(ctx, 0, 100)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	return envs
}

// TestDelegatedActorPartialRejected pins the append-time guard
// (ADR 0041 §3.4): a Via that is not complete — or a Via with nobody
// to account for — is rejected with es.ErrInvalidDelegation before
// anything is written, so the stream's version does not move. With no
// CHECK behind the writer (ADR 0041 §3.3), this validation IS the
// all-or-nothing enforcement on SQLite.
func TestDelegatedActorPartialRejected(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := newViaStream(t, "via-partial")

	// Seed one valid event: a rejection must leave an existing stream's
	// version where it was, not merely never move an empty one.
	seed := es.Actor{Type: "user", ID: "alice"}
	appendWithActor(t, ctx, s, sid, 0, seed)

	for name, actor := range map[string]es.Actor{
		"via type only":   {Type: "user", ID: "alice", Via: es.ActorRef{Type: "service"}},
		"via id only":     {Type: "user", ID: "alice", Via: es.ActorRef{ID: "agent-runner"}},
		"via, zero actor": {Via: es.ActorRef{Type: "service", ID: "agent-runner"}},
	} {
		res, err := s.Append(ctx, es.AppendParams{
			StreamID: sid,
			Actor:    actor,
			Events:   []es.EventData{{TypeURL: viaTypeURL, SchemaVersion: 1, Payload: []byte("p")}},
		})
		if !errors.Is(err, es.ErrInvalidDelegation) {
			t.Fatalf("%s: append: error = %v, want es.ErrInvalidDelegation", name, err)
		}
		if len(res.Envelopes) != 0 {
			t.Fatalf("%s: rejected append returned %d envelopes, want none", name, len(res.Envelopes))
		}
	}

	if v, err := s.CurrentStreamVersion(ctx, sid); err != nil || v != 1 {
		t.Fatalf("stream version after the rejected appends = %d (err %v), want 1 — a rejected delegation must not move the version", v, err)
	}
}

// TestDelegatedActorViaIndex pins the reverse audit query's index
// (ADR 0041 §5): migration 0010's events_via_idx must exist on events,
// must be partial (WHERE via_principal IS NOT NULL, so it stays small
// while delegation is rare), and must key on via_principal alone —
// SQLite has no workspace_id to carry into the key the way Postgres's
// partitioned table does.
func TestDelegatedActorViaIndex(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)

	rows, err := s.DB().QueryContext(ctx, `PRAGMA index_list(events)`)
	if err != nil {
		t.Fatalf("index_list: %v", err)
	}
	cols, err := rows.Columns()
	if err != nil {
		rows.Close()
		t.Fatalf("index_list columns: %v", err)
	}
	var viaIdx map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			rows.Close()
			t.Fatalf("scan index_list: %v", err)
		}
		row := make(map[string]any, len(cols))
		for i, c := range cols {
			row[c] = vals[i]
		}
		if row["name"] == "events_via_idx" {
			viaIdx = row
			break
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate index_list: %v", err)
	}
	if viaIdx == nil {
		t.Fatal("PRAGMA index_list(events) has no events_via_idx row — migration 0010's partial via index is missing")
	}
	if partial, _ := viaIdx["partial"].(int64); partial != 1 {
		t.Fatalf("events_via_idx partial = %v, want 1 (the index must be partial: WHERE via_principal IS NOT NULL)", viaIdx["partial"])
	}

	rows, err = s.DB().QueryContext(ctx, `PRAGMA index_info(events_via_idx)`)
	if err != nil {
		t.Fatalf("index_info: %v", err)
	}
	defer rows.Close()
	var indexed []string
	for rows.Next() {
		var seqno, cid int
		var name *string
		if err := rows.Scan(&seqno, &cid, &name); err != nil {
			t.Fatalf("scan index_info: %v", err)
		}
		if name == nil {
			t.Fatal("events_via_idx contains an expression column, want the plain via_principal column")
		}
		indexed = append(indexed, *name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate index_info: %v", err)
	}
	if want := []string{"via_principal"}; !slices.Equal(indexed, want) {
		t.Fatalf("events_via_idx columns = %v, want %v", indexed, want)
	}
}
