package es

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
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
	// Hash is the head row's stored hash: exactly HashSize bytes for a
	// chained head, empty for an unchained one (written before migration
	// 0002), in which case the root substitutes UnchainedHeadHash so the
	// anchor still binds the version. Any other length is a corrupt head
	// and a programming error — ValidateStreamHead rejects it, and
	// AnchorRoot panics rather than attest it.
	Hash []byte
}

// Anchor is one exported anchor row.
type Anchor struct {
	ID                 string    `json:"anchor_id"`
	CreatedAt          time.Time `json:"created_at"`
	UptoGlobalPosition uint64    `json:"upto_global_position"`
	StreamCount        uint64    `json:"stream_count"`
	RootHash           []byte    `json:"root_hash"`
	// OrderRootHash is the anchor's order root: OrderRoot over the
	// log_order rows at or below the bound, attesting the global append
	// order of every batch the anchor covers. Nil for a legacy anchor
	// recorded before log-order attestation: such an anchor verifies
	// heads-only — the order comparison skips the order fields when
	// OrderRootHash is nil.
	OrderRootHash []byte `json:"order_root_hash,omitempty"`
	// OrderFromPosition is the first position the order root attests:
	// the lowest from_position among the covered rows — 0 when the
	// anchor covers none, because everything at or below its bound
	// predates log-order attestation (the order analog of the chain's
	// unattested pre-migration rows).
	OrderFromPosition uint64 `json:"order_from_position"`
	// OrderCount is how many log_order rows the order root aggregates —
	// the order analog of StreamCount.
	OrderCount uint64 `json:"order_count"`
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
// it differs from a.RootHash. OrderRows returns the log_order rows with
// to_position <= upto (0 = the current log head, as for Anchor): the rows
// an anchor at upto attests with its OrderRootHash, which `es-verify
// order` recomputes each batch hash from. ListStreams enumerates streams
// for `es-verify all`.
type Anchorer interface {
	Anchor(ctx context.Context, upto uint64) (Anchor, error)
	ListAnchors(ctx context.Context) ([]Anchor, error)
	CheckAnchor(ctx context.Context, a Anchor) error
	ListStreams(ctx context.Context) ([]StreamRef, error)
	OrderRows(ctx context.Context, upto uint64) ([]OrderRow, error)
}

// ErrInvalidStreamHead reports that a StreamHead's Hash is neither empty
// (an unchained head) nor exactly HashSize bytes (a chained one) — a
// corrupt-length hash that must not be attested. ValidateStreamHead
// returns errors wrapping it; AnchorRoot panics on the same condition.
var ErrInvalidStreamHead = errors.New("es-lite: invalid stream head")

// ValidateStreamHead reports whether h is a well-formed anchor input: its
// Hash must be empty (an unchained head, written before migration 0002) or
// exactly HashSize bytes (a chained head). Any other length is a corrupt
// head and a programming error in the caller — the in-repo adapters never
// produce one (their heads queries read the stored hash directly), so this
// is chiefly for third-party Anchorer implementations assembling StreamHeads
// themselves. AnchorRoot keeps its no-error signature and panics on the
// same condition.
func ValidateStreamHead(h StreamHead) error {
	if n := len(h.Hash); n != 0 && n != HashSize {
		return fmt.Errorf("%w: stream %s: head hash is %d bytes, want 0 (unchained) or %d (chained)",
			ErrInvalidStreamHead, h.StreamID, n, HashSize)
	}
	return nil
}

// AnchorRoot is the anchor root: SHA-256 over the heads sorted by workspace
// first and the canonical stream_id ("type:id") second, both compared
// byte-wise as raw byte strings — never as (type, id) tuples, so an
// off-box reimplementer sorts identically. The sort is stable: heads with
// equal (workspace, canonical stream_id) keys keep their input order. The
// pre-image starts with the length-prefixed "es-lite.anchor.v1" domain
// separator (a u32 big-endian byte count, as for every length-prefixed
// field below) and a big-endian u64 head count, then encodes each head, in
// sorted order, as a length-prefixed workspace, a length-prefixed canonical
// stream_id, a big-endian u64 version and 32 hash bytes — the head's stored
// hash for a chained head, UnchainedHeadHash when the head's hash is empty
// (unchained, written before migration 0002). A
// hash that is neither empty nor exactly HashSize bytes is a programming
// error and panics: ValidateStreamHead reports the same condition as an
// error for callers that would rather reject than crash. A flat hash, not
// a Merkle tree — a per-stream membership proof is not required yet and a
// tree can replace the root later without changing the row shape.
func AnchorRoot(heads []StreamHead) [HashSize]byte {
	sorted := make([]StreamHead, len(heads))
	copy(sorted, heads)
	sort.SliceStable(sorted, func(i, j int) bool {
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
		switch len(h.Hash) {
		case HashSize:
			b = append(b, h.Hash...)
		case 0:
			u := UnchainedHeadHash(h.StreamID, h.Version)
			b = append(b, u[:]...)
		default:
			panic(fmt.Sprintf("es-lite: AnchorRoot: stream %s: head hash is %d bytes, want 0 (unchained) or %d (chained)",
				h.StreamID, len(h.Hash), HashSize))
		}
	}
	return sha256.Sum256(b)
}
