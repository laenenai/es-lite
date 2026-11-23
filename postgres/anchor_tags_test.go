package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/laenenai/es-lite/es"
)

func TestAnchorCarriesTagsRoot(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	sid, _ := es.NewStreamID("counter", "anchor-tags")

	appendTagged(t, ctx, ws, sid, 0,
		map[string]string{"flowdoc.space": "s1"},
		map[string]string{"flowdoc.block": "b1"},
	)
	// An untagged batch is excluded from the tags root and its count.
	appendTagged(t, ctx, ws, sid, 1, nil, nil)

	var from uint64
	if err := s.Pool().QueryRow(ctx, `
		SELECT from_position FROM log_order
		WHERE workspace_id = 'ws_a' AND tags_hash IS NOT NULL`).Scan(&from); err != nil {
		t.Fatalf("read tagged batch from_position: %v", err)
	}

	a, err := s.Anchor(ctx, 0)
	if err != nil {
		t.Fatalf("Anchor: %v", err)
	}
	if len(a.TagsRootHash) != es.HashSize {
		t.Fatalf("TagsRootHash length = %d, want %d", len(a.TagsRootHash), es.HashSize)
	}
	if a.TagsCount != 1 {
		t.Fatalf("TagsCount = %d, want 1", a.TagsCount)
	}
	if a.TagsFromPosition != from {
		t.Fatalf("TagsFromPosition = %d, want %d", a.TagsFromPosition, from)
	}
	if err := s.CheckAnchor(ctx, a); err != nil {
		t.Fatalf("CheckAnchor: %v", err)
	}
}

// TestCheckAnchorDetectsTamperedTag tampers the stored batch tags hash. The
// heads and order roots are untouched, so only the sibling tags root can catch
// it — the anchor attests log_order.tags_hash, while `es-verify tags` is the
// deeper audit that recomputes that hash from events.tags.
func TestCheckAnchorDetectsTamperedTag(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	sid, _ := es.NewStreamID("counter", "anchor-tamper-tags")

	appendTagged(t, ctx, ws, sid, 0,
		map[string]string{"flowdoc.space": "s1"},
		map[string]string{"flowdoc.block": "b1"},
	)
	a, err := s.Anchor(ctx, 0)
	if err != nil {
		t.Fatalf("Anchor: %v", err)
	}

	if _, err := s.Pool().Exec(ctx, `
		UPDATE log_order SET tags_hash = decode(repeat('00', 32), 'hex')
		WHERE workspace_id = 'ws_a' AND tags_hash IS NOT NULL`); err != nil {
		t.Fatalf("tamper log_order.tags_hash: %v", err)
	}
	if err := s.CheckAnchor(ctx, a); !errors.Is(err, es.ErrAnchorMismatch) {
		t.Fatalf("CheckAnchor after tag tamper = %v, want ErrAnchorMismatch", err)
	}
}

func TestLegacyAnchorWithoutTagsRootVerifies(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	sid, _ := es.NewStreamID("counter", "anchor-legacy-tags")

	appendTagged(t, ctx, ws, sid, 0,
		map[string]string{"flowdoc.space": "s1"},
		map[string]string{"flowdoc.block": "b1"},
	)
	a, err := s.Anchor(ctx, 0)
	if err != nil {
		t.Fatalf("Anchor: %v", err)
	}

	// A v0.22-style anchor: order root present, tags root absent.
	orderOnly := a
	orderOnly.TagsRootHash = nil
	orderOnly.TagsCount = 0
	orderOnly.TagsFromPosition = 0
	if err := s.CheckAnchor(ctx, orderOnly); err != nil {
		t.Fatalf("CheckAnchor on an order-only anchor: %v", err)
	}

	// A pre-order anchor: both siblings absent; heads-only verification.
	fullyLegacy := orderOnly
	fullyLegacy.OrderRootHash = nil
	fullyLegacy.OrderCount = 0
	fullyLegacy.OrderFromPosition = 0
	if err := s.CheckAnchor(ctx, fullyLegacy); err != nil {
		t.Fatalf("CheckAnchor on a fully legacy anchor: %v", err)
	}
}
