package es_test

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
	"testing"

	"github.com/laenenai/es-lite/es"
)

// Golden vectors for the tags encodings. The expected values were produced
// by an independent reimplementation of the pre-images (Python:
// int.to_bytes + hashlib), not by running this implementation. Changing any
// of these means changing the attestation format: a new domain separator,
// not an edit here.

var goldenTagEntries = []es.TagEntry{
	{Position: 42, Key: "flowdoc.space", Value: "space-1"},
	{Position: 42, Key: "flowdoc.block", Value: "block-1"},
	{Position: 43, Key: "flowdoc.space", Value: "space-1"},
}

var goldenTagsRows = []es.TagsRow{
	{From: 5, To: 7, TagsHash: mustHex("9f72ea0cf49536e3c66c787f705186df9a4378083753ae9536d65b3ad7fcddc4")},
	{From: 1, To: 4, TagsHash: mustHex("5d2d8d31f7add6d5f3165c573d8bbb0f620035eca871773d0c37dedc5c255abe")},
}

const (
	// u32("es-lite.tags.v1") ‖ u32(3) ‖, per entry sorted by
	// (position, key, value): u64(position) ‖ LP(key) ‖ LP(value).
	goldenTagsPreimage = "0000000f65732d6c6974652e746167732e763100000003000000000000002a0000000d666c6f77646f632e626c6f636b00000007626c6f636b2d31000000000000002a0000000d666c6f77646f632e73706163650000000773706163652d31000000000000002b0000000d666c6f77646f632e73706163650000000773706163652d31"
	goldenTagsHash     = "5d2d8d31f7add6d5f3165c573d8bbb0f620035eca871773d0c37dedc5c255abe"

	// u32("es-lite.tags-root.v1") ‖ u64(2) ‖, per row sorted by ascending
	// From: u64(from) ‖ u64(to) ‖ 32-byte tags hash.
	goldenTagsRootPreimage = "0000001465732d6c6974652e746167732d726f6f742e76310000000000000002000000000000000100000000000000045d2d8d31f7add6d5f3165c573d8bbb0f620035eca871773d0c37dedc5c255abe000000000000000500000000000000079f72ea0cf49536e3c66c787f705186df9a4378083753ae9536d65b3ad7fcddc4"
	goldenTagsRootHash     = "1059292641a7da566f0db0326a4fc348669a792f739fa36178d904ea2070b253"
)

func mustHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

func tagsAppendLP(b []byte, s string) []byte {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(s)))
	b = append(b, n[:]...)
	return append(b, s...)
}

func tagsPreimage(entries []es.TagEntry) []byte {
	sorted := append([]es.TagEntry(nil), entries...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Position != sorted[j].Position {
			return sorted[i].Position < sorted[j].Position
		}
		if sorted[i].Key != sorted[j].Key {
			return sorted[i].Key < sorted[j].Key
		}
		return sorted[i].Value < sorted[j].Value
	})
	b := tagsAppendLP(nil, "es-lite.tags.v1")
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(sorted)))
	b = append(b, n[:]...)
	for _, e := range sorted {
		var p [8]byte
		binary.BigEndian.PutUint64(p[:], e.Position)
		b = append(b, p[:]...)
		b = tagsAppendLP(b, e.Key)
		b = tagsAppendLP(b, e.Value)
	}
	return b
}

func tagsRootPreimage(rows []es.TagsRow) []byte {
	sorted := append([]es.TagsRow(nil), rows...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].From < sorted[j].From })
	b := tagsAppendLP(nil, "es-lite.tags-root.v1")
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(sorted)))
	b = append(b, n[:]...)
	for _, r := range sorted {
		var v [8]byte
		binary.BigEndian.PutUint64(v[:], r.From)
		b = append(b, v[:]...)
		binary.BigEndian.PutUint64(v[:], r.To)
		b = append(b, v[:]...)
		b = append(b, r.TagsHash...)
	}
	return b
}

func TestTagsHashGolden(t *testing.T) {
	pre := tagsPreimage(goldenTagEntries)
	if got := hex.EncodeToString(pre); got != goldenTagsPreimage {
		t.Fatalf("tags pre-image = %s, want %s", got, goldenTagsPreimage)
	}
	got := es.TagsHash(goldenTagEntries)
	if want := sha256.Sum256(pre); got != want {
		t.Fatalf("TagsHash = %x, want %x", got, want)
	}
	if hexGot := hex.EncodeToString(got[:]); hexGot != goldenTagsHash {
		t.Fatalf("TagsHash = %s, want %s", hexGot, goldenTagsHash)
	}
}

func TestTagsRootGolden(t *testing.T) {
	pre := tagsRootPreimage(goldenTagsRows)
	if got := hex.EncodeToString(pre); got != goldenTagsRootPreimage {
		t.Fatalf("tags root pre-image = %s, want %s", got, goldenTagsRootPreimage)
	}
	got := es.TagsRoot(goldenTagsRows)
	if want := sha256.Sum256(pre); got != want {
		t.Fatalf("TagsRoot = %x, want %x", got, want)
	}
	if hexGot := hex.EncodeToString(got[:]); hexGot != goldenTagsRootHash {
		t.Fatalf("TagsRoot = %s, want %s", hexGot, goldenTagsRootHash)
	}
}

func TestTagsHashDeterministic(t *testing.T) {
	shuffled := []es.TagEntry{
		goldenTagEntries[2],
		goldenTagEntries[0],
		goldenTagEntries[1],
	}
	if got, want := es.TagsHash(shuffled), es.TagsHash(goldenTagEntries); got != want {
		t.Fatalf("TagsHash(shuffled) = %x, want %x", got, want)
	}
}

func TestTagsRootPanicsOnBadHash(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("TagsRoot did not panic on a short tags hash")
		}
	}()
	es.TagsRoot([]es.TagsRow{{From: 1, To: 2, TagsHash: make([]byte, es.HashSize-1)}})
}
