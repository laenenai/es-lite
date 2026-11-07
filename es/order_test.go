package es_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/laenenai/es-lite/es"
)

// Golden vectors for the log-order encodings (design D1, finding F11),
// mirroring the goldens in hash_test.go and anchor_test.go: the full
// pre-image and the hash are pinned so an off-box reimplementer can
// validate their encoding against the same fixture. The expected values
// were produced by an independent reimplementation of the encodings
// (Python: int.to_bytes + hashlib, recorded in full in each test's
// comment), not just by running this implementation. Changing any of
// these means changing the attestation format — a new domain separator,
// not an edit here.

// The batch fixture: one append batch of three events — the positions it
// drew (42, 43, 44) with each event's stored chain hash.
var (
	goldenBatchEntry1 = es.OrderEntry{Position: 42, Hash: bytes.Repeat([]byte{0x11}, es.HashSize)}
	goldenBatchEntry2 = es.OrderEntry{Position: 43, Hash: bytes.Repeat([]byte{0x22}, es.HashSize)}
	goldenBatchEntry3 = es.OrderEntry{Position: 44, Hash: bytes.Repeat([]byte{0x33}, es.HashSize)}
)

// The order-root fixture: three batches' log_order rows, deliberately
// given out of order — the root's encoding order is ascending From
// (1, 5, 8), which the scrambled input must not change.
var (
	goldenOrderRowA = es.OrderRow{From: 5, To: 7, Count: 3, BatchHash: bytes.Repeat([]byte{0xAA}, es.HashSize)}
	goldenOrderRowB = es.OrderRow{From: 8, To: 8, Count: 1, BatchHash: bytes.Repeat([]byte{0xBB}, es.HashSize)}
	goldenOrderRowC = es.OrderRow{From: 1, To: 4, Count: 4, BatchHash: bytes.Repeat([]byte{0xCC}, es.HashSize)}
)

const (
	// u32("es-lite.log-order.v1") ‖ u32(3) ‖, per entry in the order
	// given: u64(position) ‖ 32B hash.
	goldenBatchPreimage = "0000001465732d6c6974652e6c6f672d6f726465722e763100000003000000000000002a1111111111111111111111111111111111111111111111111111111111111111000000000000002b2222222222222222222222222222222222222222222222222222222222222222000000000000002c3333333333333333333333333333333333333333333333333333333333333333"
	goldenBatchHash     = "a7096ae10fa55a8b95924fd55182edd9aa43d195988210ea274797f2458ce903"

	// u32("es-lite.log-order-root.v1") ‖ u64(3) ‖, per row sorted by
	// ascending From (1, 5, 8): u64(from) ‖ u64(to) ‖ u32(count) ‖ 32B
	// batch hash.
	goldenRootPreimage = "0000001965732d6c6974652e6c6f672d6f726465722d726f6f742e763100000000000000030000000000000001000000000000000400000004cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc0000000000000005000000000000000700000003aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0000000000000008000000000000000800000001bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	goldenOrderRoot    = "1602b5977cb8e0d7e01080d64e67a51da09a04d3209921641cda08fd78d13575"
)

// batchOrderPreimage encodes the BatchOrderHash pre-image straight from
// the BatchOrderHash doc comment: a u32 length-prefixed domain
// separator, a big-endian u32 entry count, then per entry — in the
// order given, as the adapter's insert loop supplies them; this helper
// does not sort — a big-endian u64 position and the 32 hash bytes.
func batchOrderPreimage(entries []es.OrderEntry) []byte {
	b := make([]byte, 0, 40*len(entries)+64)
	b = anchorAppendLP(b, "es-lite.log-order.v1")
	b = binary.BigEndian.AppendUint32(b, uint32(len(entries)))
	for _, e := range entries {
		b = binary.BigEndian.AppendUint64(b, e.Position)
		b = append(b, e.Hash...)
	}
	return b
}

// orderRootPreimage encodes the OrderRoot pre-image straight from the
// OrderRoot doc comment: a u32 length-prefixed domain separator, a
// big-endian u64 row count, then per row — which the caller must already
// hold sorted by ascending From; this helper does not sort — a
// big-endian u64 from, a big-endian u64 to, a big-endian u32 count and
// the 32 batch-hash bytes.
func orderRootPreimage(rows []es.OrderRow) []byte {
	b := make([]byte, 0, 52*len(rows)+64)
	b = anchorAppendLP(b, "es-lite.log-order-root.v1")
	b = binary.BigEndian.AppendUint64(b, uint64(len(rows)))
	for _, r := range rows {
		b = binary.BigEndian.AppendUint64(b, r.From)
		b = binary.BigEndian.AppendUint64(b, r.To)
		b = binary.BigEndian.AppendUint32(b, r.Count)
		b = append(b, r.BatchHash...)
	}
	return b
}

func TestBatchOrderHashGoldenVector(t *testing.T) {
	// Expected values produced by this independent Python computation
	// (written from the BatchOrderHash doc comment only):
	//
	//	lp = lambda s: len(s).to_bytes(4, "big") + s.encode()
	//	pi = lp("es-lite.log-order.v1") + (3).to_bytes(4, "big")
	//	pi += (42).to_bytes(8, "big") + bytes([0x11]) * 32
	//	pi += (43).to_bytes(8, "big") + bytes([0x22]) * 32
	//	pi += (44).to_bytes(8, "big") + bytes([0x33]) * 32
	//	hashlib.sha256(pi).hexdigest()
	//
	// printed the 148-byte pre-image pinned in goldenBatchPreimage and
	// 'a7096ae10fa55a8b95924fd55182edd9aa43d195988210ea274797f2458ce903'.
	entries := []es.OrderEntry{goldenBatchEntry1, goldenBatchEntry2, goldenBatchEntry3}
	pre := batchOrderPreimage(entries)
	if got := hex.EncodeToString(pre); got != goldenBatchPreimage {
		t.Fatalf("batch pre-image drifted:\n got %s\nwant %s", got, goldenBatchPreimage)
	}
	if len(pre) != 148 {
		t.Fatalf("batch pre-image length = %d, want 148", len(pre))
	}
	h := sha256.Sum256(pre)
	if got := hex.EncodeToString(h[:]); got != goldenBatchHash {
		t.Fatalf("batch order hash = %s, want %s", got, goldenBatchHash)
	}
	if es.BatchOrderHash(entries) != h {
		t.Fatal("BatchOrderHash disagrees with the documented encoding over the golden entries")
	}
	// The encoding is sequence-sensitive: entries hash in the order
	// given (the caller supplies position order), so a permuted input
	// must not reach the same hash.
	if es.BatchOrderHash([]es.OrderEntry{goldenBatchEntry2, goldenBatchEntry1, goldenBatchEntry3}) == h {
		t.Fatal("permuting the entries left the batch order hash unchanged; entries must hash in the order given")
	}
}

func TestOrderRootGoldenVector(t *testing.T) {
	// Expected values produced by this independent Python computation
	// (written from the OrderRoot doc comment only):
	//
	//	lp = lambda s: len(s).to_bytes(4, "big") + s.encode()
	//	pi = lp("es-lite.log-order-root.v1") + (3).to_bytes(8, "big")
	//	for frm, to, cnt, bh in sorted([(5, 7, 3, bytes([0xAA]) * 32),
	//	                                (8, 8, 1, bytes([0xBB]) * 32),
	//	                                (1, 4, 4, bytes([0xCC]) * 32)],
	//	                               key=lambda r: r[0]):
	//	    pi += (frm).to_bytes(8, "big") + (to).to_bytes(8, "big")
	//	    pi += (cnt).to_bytes(4, "big") + bh
	//	hashlib.sha256(pi).hexdigest()
	//
	// printed the 193-byte pre-image pinned in goldenRootPreimage and
	// '1602b5977cb8e0d7e01080d64e67a51da09a04d3209921641cda08fd78d13575'.
	// The rows are handed to OrderRoot scrambled (A, B, C): the function
	// must sort them by From before encoding.
	scrambled := []es.OrderRow{goldenOrderRowA, goldenOrderRowB, goldenOrderRowC}
	pre := orderRootPreimage([]es.OrderRow{goldenOrderRowC, goldenOrderRowA, goldenOrderRowB})
	if got := hex.EncodeToString(pre); got != goldenRootPreimage {
		t.Fatalf("order-root pre-image drifted:\n got %s\nwant %s", got, goldenRootPreimage)
	}
	if len(pre) != 193 {
		t.Fatalf("order-root pre-image length = %d, want 193", len(pre))
	}
	root := sha256.Sum256(pre)
	if got := hex.EncodeToString(root[:]); got != goldenOrderRoot {
		t.Fatalf("order root = %s, want %s", got, goldenOrderRoot)
	}
	if es.OrderRoot(scrambled) != root {
		t.Fatal("OrderRoot disagrees with the documented encoding over the golden rows")
	}
}
