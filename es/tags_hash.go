package es

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
)

// Tags attestation (design 2026-11-23, Decision 6). Tags are excluded from
// CanonicalEnvelope, so the chain does not attest them. Instead each append
// transaction hashes exactly its own tag rows into log_order.tags_hash, and
// each anchor aggregates those batch hashes into a sibling tags root. The
// split mirrors OrderRoot: the per-batch hash is the cheap in-transaction
// witness, the exported root the off-box aggregate. This file is the
// canonical encoding of both; tags_hash_test.go pins them with goldens.

// Domain separators for the tags encodings, length-prefixed into the
// pre-images below so a tags hash can never collide with a tags root or with
// the envelope, order and anchor domains in hash.go/order.go.
const (
	tagsDomain     = "es-lite.tags.v1"
	tagsRootDomain = "es-lite.tags-root.v1"
)

// TagEntry is one tag row of an append batch as TagsHash attests it: the
// event's global position and the key/value pair projected for that event.
type TagEntry struct {
	Position uint64
	Key      string
	Value    string
}

// TagsHash is a batch's tags hash: SHA-256 over exactly the append
// transaction's own tag rows, computed inside the transaction and recorded
// in its log_order row. The pre-image, with every fixed-width integer
// big-endian and every length prefix a big-endian uint32, is:
//
//	"es-lite.tags.v1"   domain separator (len-prefixed)
//	u32 count           the entry count
//	per entry, sorted by (Position, Key, Value):
//	    u64 Position ‖ len-prefixed Key ‖ len-prefixed Value
//
// The sort is inside the hash, so map iteration order and query order do not
// matter. A batch with no tags is not hashed at all: its log_order row keeps
// a NULL tags_hash. TagsHash(nil) still returns the hash of the empty set, so
// a verifier can compare an explicitly empty batch if one ever exists.
func TagsHash(entries []TagEntry) [HashSize]byte {
	sorted := make([]TagEntry, len(entries))
	copy(sorted, entries)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Position != sorted[j].Position {
			return sorted[i].Position < sorted[j].Position
		}
		if sorted[i].Key != sorted[j].Key {
			return sorted[i].Key < sorted[j].Key
		}
		return sorted[i].Value < sorted[j].Value
	})

	b := make([]byte, 0, 48*len(sorted)+64)
	b = appendString(b, tagsDomain)
	b = binary.BigEndian.AppendUint32(b, uint32(len(sorted)))
	for _, e := range sorted {
		b = binary.BigEndian.AppendUint64(b, e.Position)
		b = appendString(b, e.Key)
		b = appendString(b, e.Value)
	}
	return sha256.Sum256(b)
}

// TagsRow is one tagged append batch's log_order row as the anchor's tags
// root aggregates it: the batch's global-position range and its TagsHash.
type TagsRow struct {
	From, To uint64
	TagsHash []byte
}

// TagsRoot is the anchor's tags root: SHA-256 over the log_order rows at or
// below the anchor's bound that carry a non-NULL tags_hash. The pre-image,
// with every fixed-width integer big-endian and length prefixes as uint32, is:
//
//	"es-lite.tags-root.v1"   domain separator (len-prefixed)
//	u64 row count            big-endian, the full 64 bits
//	per row, sorted by ascending From:
//	    u64 From ‖ u64 To ‖ 32 TagsHash bytes
//
// Rows without tags are excluded entirely (their tags_hash is NULL), so an
// untagged log and a tagged log do not share a root. A new anchor over zero
// tagged rows still records the empty-set root; nil is exclusively the legacy
// marker for an anchor recorded before tags attestation. A TagsHash that is
// not exactly HashSize bytes is a programming error and panics, as OrderRoot
// does.
func TagsRoot(rows []TagsRow) [HashSize]byte {
	sorted := make([]TagsRow, len(rows))
	copy(sorted, rows)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].From < sorted[j].From })

	b := make([]byte, 0, 52*len(sorted)+64)
	b = appendString(b, tagsRootDomain)
	b = binary.BigEndian.AppendUint64(b, uint64(len(sorted)))
	for _, r := range sorted {
		b = binary.BigEndian.AppendUint64(b, r.From)
		b = binary.BigEndian.AppendUint64(b, r.To)
		switch len(r.TagsHash) {
		case HashSize:
			b = append(b, r.TagsHash...)
		default:
			panic(fmt.Sprintf("es-lite: TagsRoot: row from %d to %d: tags hash is %d bytes, want %d",
				r.From, r.To, len(r.TagsHash), HashSize))
		}
	}
	return sha256.Sum256(b)
}

// TagsRows selects the covered log_order rows that carry a tags hash, in the
// shape TagsRoot aggregates. Rows without tags (a NULL tags_hash) are excluded:
// they are the untagged batches, not empty tag sets. The caller computes the
// root over the result and sets the anchor's TagsRootHash, TagsCount and
// TagsFromPosition from it.
func TagsRows(rows []OrderRow) []TagsRow {
	out := make([]TagsRow, 0, len(rows))
	for _, r := range rows {
		if r.TagsHash != nil {
			out = append(out, TagsRow{From: r.From, To: r.To, TagsHash: r.TagsHash})
		}
	}
	return out
}
