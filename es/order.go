package es

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
)

// Log-order attestation (design D1, finding F11). The per-stream hash
// chain and the anchor root over stream heads bind what each stream
// contains; nothing bound the order in which events landed in the
// global log — a batch's positions are the only witness, and they live
// in the same rows an attacker can rewrite. Order attestation closes
// that gap in two pieces: every append transaction hashes exactly its
// own events — position and stored hash, nothing from any other
// transaction — into a batch hash recorded in a log_order row inside
// the same transaction, and each anchor aggregates those rows into an
// order root. The exported anchor then attests global append order,
// mirroring the chain/anchor split of ADR 0029: the per-batch hash is
// the cheap in-transaction witness, the order root the exported
// aggregate. This file is the canonical encoding of both hashes;
// order_test.go pins them with golden vectors.

// Domain separators for the log-order encodings, length-prefixed into
// the pre-images below so a batch hash can never collide with an order
// root or with the envelope and anchor domains in hash.go.
const (
	orderDomain     = "es-lite.log-order.v1"
	orderRootDomain = "es-lite.log-order-root.v1"
)

// OrderEntry is one event of an append batch as BatchOrderHash attests
// it: the row's global position and its stored chain hash.
type OrderEntry struct {
	Position uint64
	Hash     []byte
}

// BatchOrderHash is the batch's order hash: SHA-256 over exactly the
// append transaction's own events, computed inside the transaction and
// recorded in its log_order row — no cross-transaction reads, so the
// append path gains no new serialization (design D1). The pre-image,
// with every fixed-width integer big-endian and every length prefix a
// big-endian uint32, is:
//
//	"es-lite.log-order.v1"   domain separator (len-prefixed)
//	u32 count                the entry count
//	per entry, in the order given: u64 position ‖ 32 hash bytes
//
// Entries are encoded in the order the caller supplies, which is
// position order: the adapter feeds them straight from its insert loop
// and a verifier pages the rows back by position, both ascending. The
// function does not sort — the hash is sequence-sensitive, so a caller
// must feed entries in the order it wants attested, and both in-repo
// call sites hold ascending positions by construction. The count is a
// u32, not the u64 of OrderRoot's row count, because it is the same
// number the log_order row stores as the batch's event count.
//
// An entry Hash that is not exactly HashSize bytes is a programming
// error — a stored event hash is always a full chain hash — and panics,
// as AnchorRoot does for a corrupt head hash: attesting malformed input
// silently is worse than crashing the caller.
func BatchOrderHash(entries []OrderEntry) [HashSize]byte {
	b := make([]byte, 0, 40*len(entries)+64)
	b = appendString(b, orderDomain)
	b = binary.BigEndian.AppendUint32(b, uint32(len(entries)))
	for _, e := range entries {
		b = binary.BigEndian.AppendUint64(b, e.Position)
		switch len(e.Hash) {
		case HashSize:
			b = append(b, e.Hash...)
		default:
			panic(fmt.Sprintf("es-lite: BatchOrderHash: entry at position %d: hash is %d bytes, want %d",
				e.Position, len(e.Hash), HashSize))
		}
	}
	return sha256.Sum256(b)
}

// OrderRow is one append batch's log_order row, the aggregate unit of
// order attestation: the batch's global-position range — From is its
// first event's position, To its last — how many events it wrote, and
// its BatchOrderHash.
type OrderRow struct {
	From, To  uint64
	Count     uint32
	BatchHash []byte
}

// OrderRoot is the anchor's order root: SHA-256 over the log_order rows
// at or below the anchor's bound — the order analog of AnchorRoot. The
// pre-image, with every fixed-width integer big-endian and every length
// prefix a big-endian uint32, is:
//
//	"es-lite.log-order-root.v1"   domain separator (len-prefixed)
//	u64 row count                 big-endian, the full 64 bits
//	per row, sorted by ascending From: u64 from ‖ u64 to ‖ u32 count ‖ 32 batch-hash bytes
//
// The sort mirrors AnchorRoot's: rows are put in ascending From order
// first (a stable sort — equal From keys keep their input order, though
// real batches never share one), so the root depends only on which
// rows are covered, never on the order a query returned them in. The
// row count is a u64 — the width AnchorRoot uses for its head count,
// since an anchor may cover unboundedly many batches — where a single
// batch's event count (BatchOrderHash's u32, OrderRow's Count) is
// table-fixed.
//
// A BatchHash that is not exactly HashSize bytes is a programming error
// — every log_order row stores a full BatchOrderHash — and panics, as
// AnchorRoot does for a corrupt head hash.
func OrderRoot(rows []OrderRow) [HashSize]byte {
	sorted := make([]OrderRow, len(rows))
	copy(sorted, rows)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].From < sorted[j].From })
	b := make([]byte, 0, 52*len(sorted)+64)
	b = appendString(b, orderRootDomain)
	b = binary.BigEndian.AppendUint64(b, uint64(len(sorted)))
	for _, r := range sorted {
		b = binary.BigEndian.AppendUint64(b, r.From)
		b = binary.BigEndian.AppendUint64(b, r.To)
		b = binary.BigEndian.AppendUint32(b, r.Count)
		switch len(r.BatchHash) {
		case HashSize:
			b = append(b, r.BatchHash...)
		default:
			panic(fmt.Sprintf("es-lite: OrderRoot: row from %d to %d: batch hash is %d bytes, want %d",
				r.From, r.To, len(r.BatchHash), HashSize))
		}
	}
	return sha256.Sum256(b)
}

// OrderVerifyReport is the verdict of VerifyOrder, the deep log-order
// verification the backends expose: whether every covered log_order
// row's recomputed batch hash matched, how many rows were checked,
// and — when OK is false — the first mismatching row in from_position
// order. A broken log order is a verdict, not an error: the report
// carries it (the ChainReport precedent), and VerifyOrder's error
// return is reserved for failures to run the verification at all.
type OrderVerifyReport struct {
	OK          bool
	RowsChecked uint64
	Mismatch    *OrderRowMismatch
}

// OrderRowMismatch is the first log_order row whose recomputed
// es.BatchOrderHash differs from the stored batch_hash: the row's
// from_position, the hash the append transaction attested (the stored
// row), and the hash the events actually yield now.
type OrderRowMismatch struct {
	From              uint64
	ExpectedBatchHash []byte
	ActualBatchHash   []byte
}
