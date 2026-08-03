package sqlite_test

import (
	"context"
	"errors"
	"testing"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/sqlite"
)

func openClaimsStore(t *testing.T) *sqlite.Store {
	t.Helper()
	ctx := context.Background()
	s, err := sqlite.Open(ctx, "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestLookupClaim(t *testing.T) {
	s := openClaimsStore(t)
	ctx := context.Background()
	want, _ := es.NewStreamID("thing", "u1")

	if err := appendWith(t, s, "u1", 0, es.Claim("user.identity", "idp|sub-1", false)); err != nil {
		t.Fatal(err)
	}
	// held claim → resolves to the claiming stream
	got, ok, err := s.LookupClaim(ctx, "user.identity", "idp|sub-1", false)
	if err != nil || !ok {
		t.Fatalf("lookup held: ok=%v err=%v", ok, err)
	}
	if got != want.Canonical() {
		t.Fatalf("stream=%q want %q", got, want.Canonical())
	}
	// unknown value → not found, no error
	if _, ok, err := s.LookupClaim(ctx, "user.identity", "idp|missing", false); ok || err != nil {
		t.Fatalf("missing: ok=%v err=%v", ok, err)
	}
	// released claim → no longer found
	if err := appendWith(t, s, "u1", 1, es.Release("user.identity", "idp|sub-1", false)); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.LookupClaim(ctx, "user.identity", "idp|sub-1", false); ok {
		t.Fatal("released claim still resolves")
	}
}

// appendWith writes one dummy event plus the given constraints to a stream.
func appendWith(t *testing.T, s *sqlite.Store, streamID string, expected uint64, ops ...es.ConstraintOp) error {
	t.Helper()
	sid, err := es.NewStreamID("thing", streamID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Append(context.Background(), es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: expected,
		Events:          []es.EventData{{TypeURL: "test.v1.E", SchemaVersion: 1, Payload: []byte("x")}},
		Constraints:     ops,
	})
	return err
}

func TestClaimConflict(t *testing.T) {
	s := openClaimsStore(t)

	if err := appendWith(t, s, "s1", 0, es.Claim("user.email", "a@b.com", false)); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	// A different stream claiming the same value collides.
	err := appendWith(t, s, "s2", 0, es.Claim("user.email", "a@b.com", false))
	if !errors.Is(err, es.ErrConstraintViolated) {
		t.Fatalf("conflicting claim: got %v, want ErrConstraintViolated", err)
	}
	// The failed append persisted nothing: s2 is still empty.
	sid2, _ := es.NewStreamID("thing", "s2")
	if v, _ := s.CurrentStreamVersion(context.Background(), sid2); v != 0 {
		t.Fatalf("s2 version = %d after rolled-back append, want 0", v)
	}
}

func TestClaimDifferentScopesAndValues(t *testing.T) {
	s := openClaimsStore(t)
	// Same value in a different scope is independent.
	if err := appendWith(t, s, "s1", 0, es.Claim("user.email", "a@b.com", false)); err != nil {
		t.Fatalf("claim 1: %v", err)
	}
	if err := appendWith(t, s, "s2", 0, es.Claim("org.slug", "a@b.com", false)); err != nil {
		t.Fatalf("claim in other scope should be independent: %v", err)
	}
}

func TestRenameReleasesAndReclaims(t *testing.T) {
	s := openClaimsStore(t)

	if err := appendWith(t, s, "s1", 0, es.Claim("drn", "pascal", false)); err != nil {
		t.Fatalf("initial claim: %v", err)
	}
	// Rename s1: release "pascal", claim "pascal-2" atomically in one append.
	if err := appendWith(t, s, "s1", 1,
		es.Release("drn", "pascal", false),
		es.Claim("drn", "pascal-2", false),
	); err != nil {
		t.Fatalf("rename: %v", err)
	}
	// The old value is now free for another stream.
	if err := appendWith(t, s, "s2", 0, es.Claim("drn", "pascal", false)); err != nil {
		t.Fatalf("reclaim of released value: %v", err)
	}
	// And the new value is held: a third stream cannot take it.
	err := appendWith(t, s, "s3", 0, es.Claim("drn", "pascal-2", false))
	if !errors.Is(err, es.ErrConstraintViolated) {
		t.Fatalf("claiming the renamed-to value: got %v, want ErrConstraintViolated", err)
	}
}

func TestConflictingRenameRollsBack(t *testing.T) {
	s := openClaimsStore(t)
	if err := appendWith(t, s, "s1", 0, es.Claim("drn", "alice", false)); err != nil {
		t.Fatalf("s1 claim: %v", err)
	}
	if err := appendWith(t, s, "s2", 0, es.Claim("drn", "bob", false)); err != nil {
		t.Fatalf("s2 claim: %v", err)
	}
	// s2 tries to rename bob -> alice, but alice is held by s1. The whole
	// append must roll back: s2 keeps "bob" and gains no event.
	err := appendWith(t, s, "s2", 1,
		es.Release("drn", "bob", false),
		es.Claim("drn", "alice", false),
	)
	if !errors.Is(err, es.ErrConstraintViolated) {
		t.Fatalf("conflicting rename: got %v, want ErrConstraintViolated", err)
	}
	sid2, _ := es.NewStreamID("thing", "s2")
	if v, _ := s.CurrentStreamVersion(context.Background(), sid2); v != 1 {
		t.Fatalf("s2 version = %d, want 1 (rename rolled back)", v)
	}
	// "bob" is still held by s2 (release rolled back too).
	if err := appendWith(t, s, "s3", 0, es.Claim("drn", "bob", false)); !errors.Is(err, es.ErrConstraintViolated) {
		t.Fatalf("bob should still be held by s2: got %v", err)
	}
}
