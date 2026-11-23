package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/postgres"
)

type liveTagRow struct {
	key, value string
	position   uint64
}

func readLiveTagRows(t *testing.T, ctx context.Context, s *postgres.Store) []liveTagRow {
	t.Helper()
	rows, err := s.Pool().Query(ctx, `
		SELECT tag_key, tag_value, global_position FROM event_tags
		WHERE workspace_id = 'ws_a'
		ORDER BY global_position, tag_key, tag_value`)
	if err != nil {
		t.Fatalf("read event_tags: %v", err)
	}
	defer rows.Close()
	var out []liveTagRow
	for rows.Next() {
		var r liveTagRow
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

func TestRebuildTagsFromEventLog(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	sid, _ := es.NewStreamID("counter", "rebuild-tags")

	appendTagged(t, ctx, ws, sid, 0,
		map[string]string{"flowdoc.space": "s1"},
		map[string]string{"flowdoc.block": "b1"},
	)
	appendTagged(t, ctx, ws, sid, 1,
		map[string]string{"flowdoc.space": "s1"},
		map[string]string{"flowdoc.block": "b2"},
	)
	want := readLiveTagRows(t, ctx, s)
	if len(want) != 4 {
		t.Fatalf("seeded tag rows = %d, want 4", len(want))
	}

	if _, err := s.Pool().Exec(ctx, `DELETE FROM event_tags WHERE workspace_id = 'ws_a'`); err != nil {
		t.Fatalf("delete event_tags: %v", err)
	}
	rep, err := ws.RebuildTags(ctx)
	if err != nil {
		t.Fatalf("RebuildTags: %v", err)
	}
	if rep.Events != 2 || rep.Tags != 4 {
		t.Fatalf("report = %+v, want Events=2 Tags=4", rep)
	}
	got := readLiveTagRows(t, ctx, s)
	if len(got) != len(want) {
		t.Fatalf("rebuilt rows = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("rebuilt row %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestVerifyTagsOKAndDivergence(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	sid, _ := es.NewStreamID("counter", "verify-tags")

	appendTagged(t, ctx, ws, sid, 0,
		map[string]string{"flowdoc.space": "s1"},
		map[string]string{"flowdoc.block": "b1"},
	)

	rep, err := ws.VerifyTags(ctx)
	if err != nil {
		t.Fatalf("VerifyTags: %v", err)
	}
	if !rep.OK || rep.Events != 1 || rep.Tags != 2 {
		t.Fatalf("VerifyTags = %+v, want OK Events=1 Tags=2", rep)
	}

	if _, err := s.Pool().Exec(ctx, `
		DELETE FROM event_tags
		WHERE workspace_id = 'ws_a' AND tag_key = 'flowdoc.block'`); err != nil {
		t.Fatalf("delete one tag row: %v", err)
	}
	rep, err = ws.VerifyTags(ctx)
	if err != nil {
		t.Fatalf("VerifyTags after delete: %v", err)
	}
	if rep.OK {
		t.Fatal("VerifyTags = OK after deleting a tag row, want divergence")
	}
	if !strings.Contains(rep.FirstDivergence, "flowdoc.block") {
		t.Fatalf("FirstDivergence = %q, want the missing key", rep.FirstDivergence)
	}
}

func TestVerifyTagsDetectsGhostRow(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")

	rep, err := ws.VerifyTags(ctx)
	if err != nil {
		t.Fatalf("VerifyTags on an empty workspace: %v", err)
	}
	if !rep.OK || rep.Events != 0 || rep.Tags != 0 {
		t.Fatalf("VerifyTags on an empty workspace = %+v, want OK with zero counts", rep)
	}

	if _, err := s.Pool().Exec(ctx, `
		INSERT INTO event_tags (workspace_id, tag_key, tag_value, global_position)
		VALUES ('ws_a', 'ghost', 'v', 1)`); err != nil {
		t.Fatalf("insert ghost tag: %v", err)
	}
	rep, err = ws.VerifyTags(ctx)
	if err != nil {
		t.Fatalf("VerifyTags with a ghost row: %v", err)
	}
	if rep.OK || !strings.Contains(rep.FirstDivergence, "ghost") {
		t.Fatalf("VerifyTags with a ghost row = %+v, want a ghost divergence", rep)
	}
}

func TestRebuildRefusesNonStringValue(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	sid, _ := es.NewStreamID("counter", "rebuild-nonstring")

	appendTagged(t, ctx, ws, sid, 0,
		map[string]string{"flowdoc.space": "s1"},
		map[string]string{"flowdoc.block": "b1"},
	)
	if _, err := s.Pool().Exec(ctx, `
		UPDATE events SET tags = '{"k":1}'
		WHERE workspace_id = 'ws_a' AND stream_id = $1`, sid.Canonical()); err != nil {
		t.Fatalf("corrupt tags: %v", err)
	}
	before := len(readLiveTagRows(t, ctx, s))

	if _, err := ws.RebuildTags(ctx); err == nil {
		t.Fatal("RebuildTags with a non-string tag value returned nil, want an error")
	}
	if after := len(readLiveTagRows(t, ctx, s)); after != before {
		t.Fatalf("live tag rows changed from %d to %d after a refused rebuild", before, after)
	}
}

func TestVerifyTagHashes(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	sid, _ := es.NewStreamID("counter", "verify-tag-hashes")

	appendTagged(t, ctx, ws, sid, 0,
		map[string]string{"flowdoc.space": "s1"},
		map[string]string{"flowdoc.block": "b1"},
	)

	rep, err := s.VerifyTagHashes(ctx, 0)
	if err != nil {
		t.Fatalf("VerifyTagHashes: %v", err)
	}
	if !rep.OK || rep.RowsChecked != 1 {
		t.Fatalf("VerifyTagHashes = %+v, want OK with 1 tagged batch", rep)
	}

	// Tamper the tagged event's tags without touching log_order.
	if _, err := s.Pool().Exec(ctx, `
		UPDATE events SET tags = '{"flowdoc.space":"s1","flowdoc.block":"evil"}'
		WHERE workspace_id = 'ws_a' AND stream_id = $1 AND tags IS NOT NULL`,
		sid.Canonical()); err != nil {
		t.Fatalf("tamper tags: %v", err)
	}
	rep, err = s.VerifyTagHashes(ctx, 0)
	if err != nil {
		t.Fatalf("VerifyTagHashes after tamper: %v", err)
	}
	if rep.OK || rep.Mismatch == nil {
		t.Fatalf("VerifyTagHashes after tamper = %+v, want a mismatch", rep)
	}
	if rep.Mismatch.From == 0 {
		t.Fatalf("mismatch From = 0, want the tagged batch's from_position")
	}
}

func TestVerifyTagHashesDetectsTagOnUntaggedBatch(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	sid, _ := es.NewStreamID("counter", "verify-null-tag-hash")

	appendTagged(t, ctx, ws, sid, 0, nil, nil)
	if _, err := s.Pool().Exec(ctx, `
		UPDATE events SET tags = '{"flowdoc.space":"s1"}'
		WHERE workspace_id = 'ws_a' AND stream_id = $1`, sid.Canonical()); err != nil {
		t.Fatalf("tag the untagged event: %v", err)
	}

	rep, err := s.VerifyTagHashes(ctx, 0)
	if err != nil {
		t.Fatalf("VerifyTagHashes: %v", err)
	}
	if rep.OK || rep.Mismatch == nil {
		t.Fatalf("VerifyTagHashes = %+v, want a mismatch for a tag added to a NULL-hash batch", rep)
	}
	if rep.Mismatch.ExpectedTagsHash != nil {
		t.Fatalf("ExpectedTagsHash = %x, want nil for an unstored hash", rep.Mismatch.ExpectedTagsHash)
	}
}

func TestVerifyTagHashesDetectsTamperedHash(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	sid, _ := es.NewStreamID("counter", "verify-tag-hash-row")

	appendTagged(t, ctx, ws, sid, 0,
		map[string]string{"flowdoc.space": "s1"},
		map[string]string{"flowdoc.block": "b1"},
	)
	if _, err := s.Pool().Exec(ctx, `
		UPDATE log_order SET tags_hash = decode(repeat('00', 32), 'hex')
		WHERE workspace_id = 'ws_a'`); err != nil {
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

// TestVerifyTagsRefusesNonStringValue pins the fail-loud rule for verify as
// well as rebuild: a stored non-string tag value must be an error, not a
// silently stringified projection or a mere divergence report.
func TestVerifyTagsRefusesNonStringValue(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	sid, _ := es.NewStreamID("counter", "verify-nonstring")

	appendTagged(t, ctx, ws, sid, 0,
		map[string]string{"flowdoc.space": "s1"},
		map[string]string{"flowdoc.block": "b1"},
	)
	if _, err := s.Pool().Exec(ctx, `
		UPDATE events SET tags = '{"k":1}'
		WHERE workspace_id = 'ws_a' AND stream_id = $1`, sid.Canonical()); err != nil {
		t.Fatalf("corrupt tags: %v", err)
	}
	if _, err := ws.VerifyTags(ctx); err == nil {
		t.Fatal("VerifyTags with a non-string tag value returned nil, want an error")
	}
}

// TestTagMaintenanceRefusesInvalidVocabulary pins the full stored-tag guard:
// an object with a bad key or too many members is corruption, not a
// projection. Rebuild and verify both refuse before touching the live index.
func TestTagMaintenanceRefusesInvalidVocabulary(t *testing.T) {
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
		{"uppercase key", `{"Bad":"v"}`},
		{"over cap", string(tooManyJSON)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s := openStore(t)
			ws := s.WorkspaceStore("ws_a")
			sid, _ := es.NewStreamID("counter", "invalid-vocab-"+strings.ReplaceAll(tc.name, " ", "-"))

			appendTagged(t, ctx, ws, sid, 0,
				map[string]string{"flowdoc.space": "s1"},
				map[string]string{"flowdoc.block": "b1"},
			)
			before := len(readLiveTagRows(t, ctx, s))
			if _, err := s.Pool().Exec(ctx, `
				UPDATE events SET tags = $1
				WHERE workspace_id = 'ws_a' AND stream_id = $2`,
				tc.tags, sid.Canonical()); err != nil {
				t.Fatalf("corrupt tags: %v", err)
			}

			if _, err := ws.RebuildTags(ctx); err == nil {
				t.Fatal("RebuildTags accepted a stored tag that violates the vocabulary")
			}
			if _, err := ws.VerifyTags(ctx); err == nil {
				t.Fatal("VerifyTags accepted a stored tag that violates the vocabulary")
			}
			if after := len(readLiveTagRows(t, ctx, s)); after != before {
				t.Fatalf("live tag rows changed from %d to %d after refused maintenance", before, after)
			}
		})
	}
}

// TestVerifyTagHashesMultipleBatches pins the single-pass join's grouping:
// two tagged batches separated by an untagged one, on one stream, are each
// checked and only the tagged ones are counted.
func TestVerifyTagHashesMultipleBatches(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	sid, _ := es.NewStreamID("counter", "verify-tag-hashes-multi")

	appendTagged(t, ctx, ws, sid, 0,
		map[string]string{"flowdoc.space": "s1"},
		map[string]string{"flowdoc.block": "b1"},
		map[string]string{"flowdoc.block": "b2"},
	)
	appendTagged(t, ctx, ws, sid, 2, nil, nil)
	appendTagged(t, ctx, ws, sid, 3,
		map[string]string{"flowdoc.space": "s1"},
		map[string]string{"flowdoc.block": "b3"},
	)

	rows, err := s.Pool().Query(ctx, `
		SELECT from_position FROM log_order
		WHERE workspace_id = 'ws_a' AND tags_hash IS NOT NULL
		ORDER BY from_position`)
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

	// Remove every tag from the second tagged batch. Its stored hash stays
	// non-NULL, so the empty recomputation must disagree.
	if _, err := s.Pool().Exec(ctx, `
		UPDATE events SET tags = NULL
		WHERE workspace_id = 'ws_a' AND global_position >= $1 AND tags IS NOT NULL`,
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
