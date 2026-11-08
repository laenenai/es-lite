package es_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/laenenai/es-lite/es"
)

// Golden vector for the anchor root (ADR 0029 §4), mirroring the
// envelope-hash goldens in hash_test.go: the full pre-image and the root
// are pinned so an off-box reimplementer can validate their encoding
// against the same fixture. The expected values were cross-checked
// against an independent Python reimplementation of the AnchorRoot doc
// comment's steps (hashlib + struct packing), not just against this
// implementation. Changing any of these means changing the anchor format
// — a new domain separator, not an edit here.

// The fixture: four heads across two workspaces, one of them unchained
// (empty hash — written before migration 0002, substituted with
// UnchainedHeadHash). The canonical sort order is D, C, B, A: workspace
// "" sorts before "ws_a" byte-wise, and within "" the canonical
// stream_ids sort "audit:log" < "cart:checkout-42" < "counter:main".
var (
	goldenAnchorHeadA = es.StreamHead{Workspace: "ws_a", StreamID: es.StreamID{Type: "counter", ID: "main"}, Version: 7, Hash: bytes.Repeat([]byte{0x11}, es.HashSize)}
	goldenAnchorHeadB = es.StreamHead{StreamID: es.StreamID{Type: "counter", ID: "main"}, Version: 3, Hash: bytes.Repeat([]byte{0x22}, es.HashSize)}
	goldenAnchorHeadC = es.StreamHead{StreamID: es.StreamID{Type: "cart", ID: "checkout-42"}, Version: 1} // unchained head
	goldenAnchorHeadD = es.StreamHead{StreamID: es.StreamID{Type: "audit", ID: "log"}, Version: 12, Hash: bytes.Repeat([]byte{0x33}, es.HashSize)}
)

const (
	// u32("es-lite.anchor.v1") ‖ u64(4) ‖, per head in canonical order:
	// u32(workspace) ‖ u32-len(canonical stream_id) ‖ u64(version) ‖ 32B hash
	// (UnchainedHeadHash("cart:checkout-42", 1) for the unchained head).
	goldenAnchorPreimage = "0000001165732d6c6974652e616e63686f722e76310000000000000004000000000000000961756469743a6c6f67000000000000000c33333333333333333333333333333333333333333333333333333333333333330000000000000010636172743a636865636b6f75742d34320000000000000001a01cbfeb00386bfd0dee5e24ea6e1321e237296bd2ca8b8023bb0ec7de32b30c000000000000000c636f756e7465723a6d61696e000000000000000322222222222222222222222222222222222222222222222222222222222222220000000477735f610000000c636f756e7465723a6d61696e00000000000000071111111111111111111111111111111111111111111111111111111111111111"
	goldenAnchorRoot     = "feaf7bcb3301885e75e58813b4f522a7ccea8855b59a83ec8b63ab78af87c1e1"
)

// anchorPreimage encodes the AnchorRoot pre-image straight from the
// AnchorRoot doc comment: a u32 length-prefixed domain separator, a
// big-endian u64 head count, then per head — which the caller must
// already hold in canonical sort order; this helper does not sort — a
// u32 length-prefixed workspace, a u32 length-prefixed canonical
// stream_id, a big-endian u64 version and 32 hash bytes, with
// UnchainedHeadHash substituted for an empty head hash.
func anchorPreimage(heads []es.StreamHead) []byte {
	b := make([]byte, 0, 96*len(heads)+32)
	b = anchorAppendLP(b, "es-lite.anchor.v1")
	b = binary.BigEndian.AppendUint64(b, uint64(len(heads)))
	for _, h := range heads {
		b = anchorAppendLP(b, h.Workspace)
		b = anchorAppendLP(b, h.StreamID.Canonical())
		b = binary.BigEndian.AppendUint64(b, h.Version)
		if len(h.Hash) == 0 {
			u := es.UnchainedHeadHash(h.StreamID, h.Version)
			b = append(b, u[:]...)
		} else {
			b = append(b, h.Hash...)
		}
	}
	return b
}

func anchorAppendLP(b []byte, s string) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(s)))
	return append(b, s...)
}

func TestAnchorRootGoldenVector(t *testing.T) {
	sorted := []es.StreamHead{goldenAnchorHeadD, goldenAnchorHeadC, goldenAnchorHeadB, goldenAnchorHeadA}
	pre := anchorPreimage(sorted)
	if got := hex.EncodeToString(pre); got != goldenAnchorPreimage {
		t.Fatalf("anchor pre-image drifted:\n got %s\nwant %s", got, goldenAnchorPreimage)
	}
	if len(pre) != 274 {
		t.Fatalf("anchor pre-image length = %d, want 274", len(pre))
	}
	root := sha256.Sum256(pre)
	if got := hex.EncodeToString(root[:]); got != goldenAnchorRoot {
		t.Fatalf("anchor root = %s, want %s", got, goldenAnchorRoot)
	}
	// The same heads in a non-canonical order must reach the identical
	// root: AnchorRoot sorts, the fixture order does not matter.
	if es.AnchorRoot([]es.StreamHead{goldenAnchorHeadB, goldenAnchorHeadA, goldenAnchorHeadD, goldenAnchorHeadC}) != root {
		t.Fatal("AnchorRoot disagrees with the documented encoding over the golden heads")
	}
}

func TestValidateStreamHeadRejectsCorruptHash(t *testing.T) {
	sid := es.StreamID{Type: "counter", ID: "main"}
	for _, tc := range []struct {
		name string
		hash []byte
	}{
		{"nil hash (unchained head)", nil},
		{"empty hash (unchained head)", []byte{}},
		{"full hash (chained head)", bytes.Repeat([]byte{0x22}, es.HashSize)},
	} {
		if err := es.ValidateStreamHead(es.StreamHead{StreamID: sid, Version: 3, Hash: tc.hash}); err != nil {
			t.Fatalf("%s: ValidateStreamHead = %v, want nil", tc.name, err)
		}
	}

	// A 16-byte head hash is corrupt, not unchained: ValidateStreamHead
	// must reject it (error message carries sizes only, never hash bytes).
	corrupt := es.StreamHead{StreamID: sid, Version: 3, Hash: bytes.Repeat([]byte{0x22}, 16)}
	err := es.ValidateStreamHead(corrupt)
	if err == nil {
		t.Fatal("16-byte head hash passed ValidateStreamHead")
	}
	if !errors.Is(err, es.ErrInvalidStreamHead) {
		t.Fatalf("ValidateStreamHead error = %v, want an error wrapping ErrInvalidStreamHead", err)
	}

	// The same corrupt head must not be silently substituted with
	// UnchainedHeadHash (F29). AnchorRoot keeps its no-error signature, so
	// it panics instead of attesting a corrupt-length hash.
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("AnchorRoot silently attested a corrupt-length head hash")
			}
		}()
		es.AnchorRoot([]es.StreamHead{goldenAnchorHeadB, corrupt})
	}()
}

func TestAnchorRootSortIsBytewiseOnCanonical(t *testing.T) {
	// The discriminating pair: as (type, id) tuples ("a","z") sorts
	// before ("a-b","a"), but byte-wise on the canonical strings "a-b:a"
	// sorts before "a:z" — '-' (0x2d) < ':' (0x3a). The doc comment pins
	// the byte-wise order; a tuple-wise comparator would flip these.
	x := es.StreamHead{StreamID: es.StreamID{Type: "a", ID: "z"}, Version: 1, Hash: bytes.Repeat([]byte{0x44}, es.HashSize)}
	y := es.StreamHead{StreamID: es.StreamID{Type: "a-b", ID: "a"}, Version: 2, Hash: bytes.Repeat([]byte{0x55}, es.HashSize)}
	byBytes := es.AnchorRoot([]es.StreamHead{x, y})
	if want := sha256.Sum256(anchorPreimage([]es.StreamHead{y, x})); byBytes != want {
		t.Fatal("AnchorRoot did not sort byte-wise on the canonical stream_id string")
	}
	if byTuple := sha256.Sum256(anchorPreimage([]es.StreamHead{x, y})); byBytes == byTuple {
		t.Fatal("fixture pair does not discriminate byte order from tuple order")
	}

	// The sort is stable (sort.SliceStable, not sort.Slice): heads with
	// equal (workspace, canonical stream_id) keys keep their input order,
	// so equal-key duplicates hash in the order they were given —
	// deterministically, not by the grace of the sort implementation.
	a5 := es.StreamHead{StreamID: es.StreamID{Type: "dup", ID: "1"}, Version: 5, Hash: bytes.Repeat([]byte{0x66}, es.HashSize)}
	a9 := es.StreamHead{StreamID: es.StreamID{Type: "dup", ID: "1"}, Version: 9, Hash: bytes.Repeat([]byte{0x77}, es.HashSize)}
	if got := es.AnchorRoot([]es.StreamHead{a5, a9}); got != sha256.Sum256(anchorPreimage([]es.StreamHead{a5, a9})) {
		t.Fatal("equal-key heads were not hashed in input order")
	}
	if got := es.AnchorRoot([]es.StreamHead{a9, a5}); got != sha256.Sum256(anchorPreimage([]es.StreamHead{a9, a5})) {
		t.Fatal("permuting equal-key heads changed the hashed order (the sort must be stable)")
	}
}
