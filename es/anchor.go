package es

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"time"
)

// Anchors (ADR 0029 §4) fix every stream's head outside the database at a
// point in time. A keyless chain proves internal consistency; an anchor the
// operator exports somewhere the database operator cannot rewrite is what
// makes alteration and tail truncation evident later.

// StreamHead is one stream's position at anchoring time.
type StreamHead struct {
	Workspace string
	StreamID  StreamID
	Version   uint64
	// Hash is the head row's stored hash; nil when the head is unchained
	// (written before migration 0002), in which case the root uses
	// UnchainedHeadHash so the anchor still binds the version.
	Hash []byte
}

// Anchor is one exported anchor row.
type Anchor struct {
	ID                 string    `json:"anchor_id"`
	CreatedAt          time.Time `json:"created_at"`
	UptoGlobalPosition uint64    `json:"upto_global_position"`
	StreamCount        uint64    `json:"stream_count"`
	RootHash           []byte    `json:"root_hash"`
}

// StreamRef names a stream across workspaces, for store-wide operations.
type StreamRef struct {
	Workspace string
	StreamID  StreamID
}

// Anchorer is implemented by backends that can compute and record anchors.
// Anchor computes the root over every stream with global_position <= upto
// (0 = the current log head), records it, and returns it. CheckAnchor
// recomputes a.UptoGlobalPosition's root and returns ErrAnchorMismatch when
// it differs from a.RootHash. ListStreams enumerates streams for
// `es-verify all`.
type Anchorer interface {
	Anchor(ctx context.Context, upto uint64) (Anchor, error)
	ListAnchors(ctx context.Context) ([]Anchor, error)
	CheckAnchor(ctx context.Context, a Anchor) error
	ListStreams(ctx context.Context) ([]StreamRef, error)
}

// AnchorRoot is the anchor root: SHA-256 over the heads sorted by
// (workspace, stream_id), each encoded as len-prefixed workspace,
// len-prefixed canonical stream_id, big-endian u64 version and 32 hash
// bytes (UnchainedHeadHash for an unchained head). A flat hash, not a
// Merkle tree — a per-stream membership proof is not required yet and a
// tree can replace the root later without changing the row shape.
func AnchorRoot(heads []StreamHead) [HashSize]byte {
	sorted := make([]StreamHead, len(heads))
	copy(sorted, heads)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Workspace != sorted[j].Workspace {
			return sorted[i].Workspace < sorted[j].Workspace
		}
		return sorted[i].StreamID.Canonical() < sorted[j].StreamID.Canonical()
	})
	b := make([]byte, 0, 96*len(sorted)+32)
	b = appendString(b, "es-lite.anchor.v1")
	b = binary.BigEndian.AppendUint64(b, uint64(len(sorted)))
	for _, h := range sorted {
		b = appendString(b, h.Workspace)
		b = appendString(b, h.StreamID.Canonical())
		b = binary.BigEndian.AppendUint64(b, h.Version)
		if len(h.Hash) == HashSize {
			b = append(b, h.Hash...)
		} else {
			u := UnchainedHeadHash(h.StreamID, h.Version)
			b = append(b, u[:]...)
		}
	}
	return sha256.Sum256(b)
}
