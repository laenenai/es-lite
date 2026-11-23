package sqlite_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/sqlite"
)

type sqliteLiveTagRow struct {
	key, value string
	position   int64
}

func sqliteLiveTagRows(t *testing.T, ctx context.Context, s *sqlite.Store) []sqliteLiveTagRow {
	t.Helper()
	rows, err := s.DB().QueryContext(ctx, `
		SELECT tag_key, tag_value, global_position FROM event_tags
		ORDER BY global_position, tag_key, tag_value`)
	if err != nil {
		t.Fatalf("read event_tags: %v", err)
	}
	defer rows.Close()
	var out []sqliteLiveTagRow
	for rows.Next() {
		var r sqliteLiveTagRow
		if err := rows.Scan(&r.key, &r.value, &r.position); err != nil {
			t.Fatalf("scan event_tags: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("event_tags rows: %v", err)
	}
	return out
}

func TestSQLiteRebuildTagsFromEventLog(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, _ := es.NewStreamID("counter", "rebuild-tags")

	if _, err := s.Append(ctx, es.AppendParams{
		StreamID: sid,
		Events: []es.EventData{
			{TypeURL: "thing.v1.Happened", Payload: []byte("1"), Tags: map[string]string{"flowdoc.block": "b1"}},
			{TypeURL: "thing.v1.Happened", Payload: []byte("2"), Tags: map[string]string{"flowdoc.block": "b2"}},
		},
		Tags: map[string]string{"flowdoc.space": "s1"},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	want := sqliteLiveTagRows(t, ctx, s)
	if len(want) != 4 {
		t.Fatalf("seeded tag rows = %d, want 4", len(want))
	}

	if _, err := s.DB().ExecContext(ctx, `DELETE FROM event_tags`); err != nil {
		t.Fatalf("delete event_tags: %v", err)
	}
	rep, err := s.RebuildTags(ctx)
	if err != nil {
		t.Fatalf("RebuildTags: %v", err)
	}
	if rep.Events != 2 || rep.Tags != 4 {
		t.Fatalf("report = %+v, want Events=2 Tags=4", rep)
	}
	got := sqliteLiveTagRows(t, ctx, s)
	if len(got) != len(want) {
		t.Fatalf("rebuilt rows = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("rebuilt row %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestSQLiteVerifyTagsOKAndDivergence(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, _ := es.NewStreamID("counter", "verify-tags")

	if _, err := s.Append(ctx, es.AppendParams{
		StreamID: sid,
		Events: []es.EventData{{
			TypeURL: "thing.v1.Happened",
			Payload: []byte("1"),
			Tags:    map[string]string{"flowdoc.block": "b1"},
		}},
		Tags: map[string]string{"flowdoc.space": "s1"},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	rep, err := s.VerifyTags(ctx)
	if err != nil {
		t.Fatalf("VerifyTags: %v", err)
	}
	if !rep.OK || rep.Events != 1 || rep.Tags != 2 {
		t.Fatalf("VerifyTags = %+v, want OK Events=1 Tags=2", rep)
	}

	if _, err := s.DB().ExecContext(ctx, `DELETE FROM event_tags WHERE tag_key = 'flowdoc.block'`); err != nil {
		t.Fatalf("delete one tag: %v", err)
	}
	rep, err = s.VerifyTags(ctx)
	if err != nil {
		t.Fatalf("VerifyTags after delete: %v", err)
	}
	if rep.OK || !strings.Contains(rep.FirstDivergence, "flowdoc.block") {
		t.Fatalf("VerifyTags after delete = %+v, want a flowdoc.block divergence", rep)
	}
}

func TestSQLiteVerifyTagsDetectsGhostRow(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	rep, err := s.VerifyTags(ctx)
	if err != nil {
		t.Fatalf("VerifyTags on an empty store: %v", err)
	}
	if !rep.OK || rep.Events != 0 || rep.Tags != 0 {
		t.Fatalf("VerifyTags on an empty store = %+v, want OK with zero counts", rep)
	}
	if _, err := s.DB().ExecContext(ctx,
		`INSERT INTO event_tags (tag_key, tag_value, global_position) VALUES ('ghost', 'v', 1)`); err != nil {
		t.Fatalf("insert ghost: %v", err)
	}
	rep, err = s.VerifyTags(ctx)
	if err != nil {
		t.Fatalf("VerifyTags with ghost: %v", err)
	}
	if rep.OK || !strings.Contains(rep.FirstDivergence, "ghost") {
		t.Fatalf("VerifyTags with ghost = %+v, want a ghost divergence", rep)
	}
}

func TestSQLiteRebuildRefusesNonStringValue(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, _ := es.NewStreamID("counter", "rebuild-nonstring")

	if _, err := s.Append(ctx, es.AppendParams{
		StreamID: sid,
		Events: []es.EventData{{
			TypeURL: "thing.v1.Happened",
			Payload: []byte("1"),
			Tags:    map[string]string{"flowdoc.block": "b1"},
		}},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := s.DB().ExecContext(ctx, `UPDATE events SET tags = '{"k":1}'`); err != nil {
		t.Fatalf("corrupt tags: %v", err)
	}
	before := len(sqliteLiveTagRows(t, ctx, s))

	if _, err := s.RebuildTags(ctx); err == nil {
		t.Fatal("RebuildTags with a non-string tag value returned nil, want an error")
	}
	if after := len(sqliteLiveTagRows(t, ctx, s)); after != before {
		t.Fatalf("live tag rows changed from %d to %d after a refused rebuild", before, after)
	}
}

func TestSQLiteVerifyTagsRefusesNonStringValue(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, _ := es.NewStreamID("counter", "verify-nonstring")

	if _, err := s.Append(ctx, es.AppendParams{
		StreamID: sid,
		Events: []es.EventData{{
			TypeURL: "thing.v1.Happened",
			Payload: []byte("1"),
			Tags:    map[string]string{"flowdoc.block": "b1"},
		}},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := s.DB().ExecContext(ctx, `UPDATE events SET tags = '{"k":1}'`); err != nil {
		t.Fatalf("corrupt tags: %v", err)
	}
	if _, err := s.VerifyTags(ctx); err == nil {
		t.Fatal("VerifyTags with a non-string tag value returned nil, want an error (or a divergence)")
	}
}

func TestSQLiteVerifyTagHashes(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, _ := es.NewStreamID("counter", "verify-tag-hashes")

	if _, err := s.Append(ctx, es.AppendParams{
		StreamID: sid,
		Events: []es.EventData{{
			TypeURL: "thing.v1.Happened",
			Payload: []byte("1"),
			Tags:    map[string]string{"flowdoc.space": "s1"},
		}},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	rep, err := s.VerifyTagHashes(ctx, 0)
	if err != nil {
		t.Fatalf("VerifyTagHashes: %v", err)
	}
	if !rep.OK || rep.RowsChecked != 1 {
		t.Fatalf("VerifyTagHashes = %+v, want OK with 1 tagged batch", rep)
	}

	if _, err := s.DB().ExecContext(ctx,
		`UPDATE events SET tags = '{"flowdoc.space":"evil"}'`); err != nil {
		t.Fatalf("tamper tags: %v", err)
	}
	rep, err = s.VerifyTagHashes(ctx, 0)
	if err != nil {
		t.Fatalf("VerifyTagHashes after tamper: %v", err)
	}
	if rep.OK || rep.Mismatch == nil {
		t.Fatalf("VerifyTagHashes after tamper = %+v, want a mismatch", rep)
	}
}

func TestSQLiteVerifyTagHashesDetectsTagOnUntaggedBatch(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, _ := es.NewStreamID("counter", "verify-null-tag-hash")

	if _, err := s.Append(ctx, es.AppendParams{
		StreamID: sid,
		Events:   []es.EventData{{TypeURL: "thing.v1.Happened", Payload: []byte("1")}},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := s.DB().ExecContext(ctx,
		`UPDATE events SET tags = '{"flowdoc.space":"s1"}'`); err != nil {
		t.Fatalf("tag the untagged event: %v", err)
	}
	rep, err := s.VerifyTagHashes(ctx, 0)
	if err != nil {
		t.Fatalf("VerifyTagHashes: %v", err)
	}
	if rep.OK || rep.Mismatch == nil || rep.Mismatch.ExpectedTagsHash != nil {
		t.Fatalf("VerifyTagHashes = %+v, want a mismatch with a nil expected hash", rep)
	}
}

func TestSQLiteVerifyTagHashesDetectsTamperedHash(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, _ := es.NewStreamID("counter", "verify-tag-hash-row")

	if _, err := s.Append(ctx, es.AppendParams{
		StreamID: sid,
		Events: []es.EventData{{
			TypeURL: "thing.v1.Happened",
			Payload: []byte("1"),
			Tags:    map[string]string{"flowdoc.space": "s1"},
		}},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := s.DB().ExecContext(ctx,
		`UPDATE log_order SET tags_hash = zeroblob(32)`); err != nil {
		t.Fatalf("tamper log_order.tags_hash: %v", err)
	}
	rep, err := s.VerifyTagHashes(ctx, 0)
	if err != nil {
		t.Fatalf("VerifyTagHashes: %v", err)
	}
	if rep.OK || rep.Mismatch == nil {
		t.Fatalf("VerifyTagHashes = %+v, want a mismatch", rep)
	}
}

// TestSQLiteTagMaintenanceRefusesInvalidVocabulary pins the strict stored-tag
// guard on SQLite: a JSON string array (which json_each would expand into
// synthetic keys), a bad key, an over-cap object, an empty string and the
// literal "null" are all corruption. Rebuild and verify refuse before
// touching the live index.
func TestSQLiteTagMaintenanceRefusesInvalidVocabulary(t *testing.T) {
	tooMany := make(map[string]string, es.MaxTagsPerEvent+1)
	for i := 0; i <= es.MaxTagsPerEvent; i++ {
		tooMany[fmt.Sprintf("k%d", i)] = "v"
	}
	tooManyJSON, err := json.Marshal(tooMany)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		tags string
	}{
		{"string array", `["a","b"]`},
		{"uppercase key", `{"Bad":"v"}`},
		{"over cap", string(tooManyJSON)},
		{"empty string", ``},
		{"literal null", `null`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s := openStore(t)
			sid, _ := es.NewStreamID("counter", "invalid-vocab-"+strings.ReplaceAll(tc.name, " ", "-"))

			if _, err := s.Append(ctx, es.AppendParams{
				StreamID: sid,
				Events: []es.EventData{{
					TypeURL: "thing.v1.Happened",
					Payload: []byte("1"),
					Tags:    map[string]string{"flowdoc.block": "b1"},
				}},
				Tags: map[string]string{"flowdoc.space": "s1"},
			}); err != nil {
				t.Fatalf("Append: %v", err)
			}
			before := len(sqliteLiveTagRows(t, ctx, s))
			if _, err := s.DB().ExecContext(ctx, `UPDATE events SET tags = ?`, tc.tags); err != nil {
				t.Fatalf("corrupt tags: %v", err)
			}

			if _, err := s.RebuildTags(ctx); err == nil {
				t.Fatal("RebuildTags accepted a stored tag that violates the vocabulary")
			}
			if _, err := s.VerifyTags(ctx); err == nil {
				t.Fatal("VerifyTags accepted a stored tag that violates the vocabulary")
			}
			if after := len(sqliteLiveTagRows(t, ctx, s)); after != before {
				t.Fatalf("live tag rows changed from %d to %d after refused maintenance", before, after)
			}
		})
	}
}

// TestSQLiteVerifyTagHashesMultipleBatches pins the single-pass join's
// grouping across tagged and untagged batches.
func TestSQLiteVerifyTagHashesMultipleBatches(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, _ := es.NewStreamID("counter", "verify-tag-hashes-multi")

	if _, err := s.Append(ctx, es.AppendParams{
		StreamID: sid,
		Events: []es.EventData{
			{TypeURL: "thing.v1.Happened", Payload: []byte("1"), Tags: map[string]string{"flowdoc.block": "b1"}},
			{TypeURL: "thing.v1.Happened", Payload: []byte("2"), Tags: map[string]string{"flowdoc.block": "b2"}},
		},
		Tags: map[string]string{"flowdoc.space": "s1"},
	}); err != nil {
		t.Fatalf("append tagged 1: %v", err)
	}
	if _, err := s.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 2,
		Events:          []es.EventData{{TypeURL: "thing.v1.Happened", Payload: []byte("3")}},
	}); err != nil {
		t.Fatalf("append untagged: %v", err)
	}
	if _, err := s.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 3,
		Events: []es.EventData{{
			TypeURL: "thing.v1.Happened", Payload: []byte("4"),
			Tags: map[string]string{"flowdoc.block": "b3"},
		}},
		Tags: map[string]string{"flowdoc.space": "s1"},
	}); err != nil {
		t.Fatalf("append tagged 2: %v", err)
	}

	rows, err := s.DB().QueryContext(ctx,
		`SELECT from_position FROM log_order WHERE tags_hash IS NOT NULL ORDER BY from_position`)
	if err != nil {
		t.Fatalf("read tagged batches: %v", err)
	}
	defer rows.Close()
	var froms []uint64
	for rows.Next() {
		var from uint64
		if err := rows.Scan(&from); err != nil {
			t.Fatalf("scan batch: %v", err)
		}
		froms = append(froms, from)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("batch rows: %v", err)
	}
	if len(froms) != 2 {
		t.Fatalf("tagged batches = %v, want 2", froms)
	}

	rep, err := s.VerifyTagHashes(ctx, 0)
	if err != nil {
		t.Fatalf("VerifyTagHashes: %v", err)
	}
	if !rep.OK || rep.RowsChecked != 2 {
		t.Fatalf("VerifyTagHashes = %+v, want OK with 2 tagged batches", rep)
	}

	if _, err := s.DB().ExecContext(ctx,
		`UPDATE events SET tags = NULL WHERE global_position >= ? AND tags IS NOT NULL`,
		int64(froms[1])); err != nil {
		t.Fatalf("remove tags: %v", err)
	}
	rep, err = s.VerifyTagHashes(ctx, 0)
	if err != nil {
		t.Fatalf("VerifyTagHashes after removal: %v", err)
	}
	if rep.OK || rep.Mismatch == nil || rep.Mismatch.From != froms[1] {
		t.Fatalf("VerifyTagHashes after removal = %+v, want mismatch at %d", rep, froms[1])
	}
}
