package sqlite_test

import (
	"context"
	"errors"
	"testing"

	"github.com/laenenai/es-lite/es"
)

func TestSQLiteAnchorTagsRoot(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, _ := es.NewStreamID("counter", "anchor-tags")

	if _, err := s.Append(ctx, es.AppendParams{
		StreamID: sid,
		Events: []es.EventData{{
			TypeURL: "thing.v1.Happened",
			Payload: []byte("1"),
			Tags:    map[string]string{"flowdoc.block": "b1"},
		}},
		Tags: map[string]string{"flowdoc.space": "s1"},
	}); err != nil {
		t.Fatalf("append tagged: %v", err)
	}
	if _, err := s.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 1,
		Events:          []es.EventData{{TypeURL: "thing.v1.Happened", Payload: []byte("2")}},
	}); err != nil {
		t.Fatalf("append untagged: %v", err)
	}

	var from int64
	if err := s.DB().QueryRowContext(ctx,
		`SELECT from_position FROM log_order WHERE tags_hash IS NOT NULL`).Scan(&from); err != nil {
		t.Fatalf("read tagged from_position: %v", err)
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
	if a.TagsFromPosition != uint64(from) {
		t.Fatalf("TagsFromPosition = %d, want %d", a.TagsFromPosition, from)
	}
	if err := s.CheckAnchor(ctx, a); err != nil {
		t.Fatalf("CheckAnchor: %v", err)
	}
}

func TestSQLiteCheckAnchorDetectsTamperedTag(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, _ := es.NewStreamID("counter", "anchor-tamper-tags")

	if _, err := s.Append(ctx, es.AppendParams{
		StreamID: sid,
		Events: []es.EventData{{
			TypeURL: "thing.v1.Happened",
			Payload: []byte("1"),
			Tags:    map[string]string{"flowdoc.block": "b1"},
		}},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	a, err := s.Anchor(ctx, 0)
	if err != nil {
		t.Fatalf("Anchor: %v", err)
	}
	if _, err := s.DB().ExecContext(ctx, `UPDATE log_order SET tags_hash = zeroblob(32)`); err != nil {
		t.Fatalf("tamper log_order.tags_hash: %v", err)
	}
	if err := s.CheckAnchor(ctx, a); !errors.Is(err, es.ErrAnchorMismatch) {
		t.Fatalf("CheckAnchor after tag tamper = %v, want ErrAnchorMismatch", err)
	}
}

func TestSQLiteLegacyAnchorWithoutTagsRoot(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, _ := es.NewStreamID("counter", "anchor-legacy-tags")

	if _, err := s.Append(ctx, es.AppendParams{
		StreamID: sid,
		Events: []es.EventData{{
			TypeURL: "thing.v1.Happened",
			Payload: []byte("1"),
			Tags:    map[string]string{"flowdoc.block": "b1"},
		}},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	a, err := s.Anchor(ctx, 0)
	if err != nil {
		t.Fatalf("Anchor: %v", err)
	}

	orderOnly := a
	orderOnly.TagsRootHash = nil
	orderOnly.TagsCount = 0
	orderOnly.TagsFromPosition = 0
	if err := s.CheckAnchor(ctx, orderOnly); err != nil {
		t.Fatalf("CheckAnchor on an order-only anchor: %v", err)
	}

	fullyLegacy := orderOnly
	fullyLegacy.OrderRootHash = nil
	fullyLegacy.OrderCount = 0
	fullyLegacy.OrderFromPosition = 0
	if err := s.CheckAnchor(ctx, fullyLegacy); err != nil {
		t.Fatalf("CheckAnchor on a fully legacy anchor: %v", err)
	}
}

// TestSQLiteAnchorOverZeroTaggedRows pins the empty-set contract: a new anchor
// over an untagged log still records a real tags root, not the nil legacy
// marker, with zero covered batches.
func TestSQLiteAnchorOverZeroTaggedRows(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, _ := es.NewStreamID("counter", "anchor-untagged")

	if _, err := s.Append(ctx, es.AppendParams{
		StreamID: sid,
		Events:   []es.EventData{{TypeURL: "thing.v1.Happened", Payload: []byte("1")}},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	a, err := s.Anchor(ctx, 0)
	if err != nil {
		t.Fatalf("Anchor: %v", err)
	}
	if len(a.TagsRootHash) != es.HashSize {
		t.Fatalf("TagsRootHash length = %d, want a real %d-byte root", len(a.TagsRootHash), es.HashSize)
	}
	if a.TagsCount != 0 || a.TagsFromPosition != 0 {
		t.Fatalf("tags coverage = (%d, %d), want (0, 0)", a.TagsCount, a.TagsFromPosition)
	}
	if err := s.CheckAnchor(ctx, a); err != nil {
		t.Fatalf("CheckAnchor: %v", err)
	}
}
