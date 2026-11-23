package sqlite_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/sqlite"
)

func tableColumns(t *testing.T, ctx context.Context, s interface {
	DB() *sql.DB
}, table string) map[string]string {
	t.Helper()
	rows, err := s.DB().QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		t.Fatalf("PRAGMA table_info(%s): %v", table, err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var (
			cid     int
			name    string
			typ     string
			notNull int
			dflt    sql.NullString
			pk      int
		)
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
			t.Fatalf("scan %s: %v", table, err)
		}
		out[name] = typ
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s rows: %v", table, err)
	}
	return out
}

func TestSQLiteEventTagsSchema(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)

	events := tableColumns(t, ctx, s, "events")
	if _, ok := events["tags"]; !ok {
		t.Fatalf("events has no tags column: %v", events)
	}
	tags := tableColumns(t, ctx, s, "event_tags")
	for _, col := range []string{"tag_key", "tag_value", "global_position"} {
		if _, ok := tags[col]; !ok {
			t.Fatalf("event_tags has no %s column: %v", col, tags)
		}
	}
	order := tableColumns(t, ctx, s, "log_order")
	if _, ok := order["tags_hash"]; !ok {
		t.Fatalf("log_order has no tags_hash column: %v", order)
	}

	rows, err := s.DB().QueryContext(ctx, `PRAGMA index_list(event_tags)`)
	if err != nil {
		t.Fatalf("PRAGMA index_list: %v", err)
	}
	defer rows.Close()
	unique := false
	for rows.Next() {
		var (
			seq     int
			name    string
			isUniq  int
			origin  string
			partial int
		)
		if err := rows.Scan(&seq, &name, &isUniq, &origin, &partial); err != nil {
			t.Fatalf("scan index_list: %v", err)
		}
		if isUniq == 1 {
			unique = true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("index_list rows: %v", err)
	}
	if !unique {
		t.Fatal("event_tags has no unique index for its primary key")
	}

	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
}

func TestSQLiteAppendPersistsTags(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, _ := es.NewStreamID("counter", "tagged")

	res, err := s.Append(ctx, es.AppendParams{
		StreamID: sid,
		Events: []es.EventData{
			{TypeURL: "thing.v1.Happened", SchemaVersion: 1, Payload: []byte("1"), Tags: map[string]string{"flowdoc.block": "b1"}},
			{TypeURL: "thing.v1.Happened", SchemaVersion: 1, Payload: []byte("2"), Tags: map[string]string{"flowdoc.block": "b2"}},
		},
		Tags: map[string]string{"flowdoc.space": "s1"},
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	for i, wantBlock := range []string{"b1", "b2"} {
		if got := res.Envelopes[i].Tags["flowdoc.space"]; got != "s1" {
			t.Fatalf("result %d space = %q, want s1", i, got)
		}
		if got := res.Envelopes[i].Tags["flowdoc.block"]; got != wantBlock {
			t.Fatalf("result %d block = %q, want %q", i, got, wantBlock)
		}
	}

	envs, err := s.ReadStream(ctx, sid, 0, 0)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	for i, wantBlock := range []string{"b1", "b2"} {
		if got := envs[i].Tags["flowdoc.space"]; got != "s1" {
			t.Fatalf("read %d space = %q, want s1", i, got)
		}
		if got := envs[i].Tags["flowdoc.block"]; got != wantBlock {
			t.Fatalf("read %d block = %q, want %q", i, got, wantBlock)
		}
	}

	var n int
	if err := s.DB().QueryRowContext(ctx, `SELECT count(*) FROM event_tags`).Scan(&n); err != nil {
		t.Fatalf("count event_tags: %v", err)
	}
	if n != 4 {
		t.Fatalf("event_tags rows = %d, want 4", n)
	}
	var tagsHash []byte
	if err := s.DB().QueryRowContext(ctx, `SELECT tags_hash FROM log_order`).Scan(&tagsHash); err != nil {
		t.Fatalf("read tags_hash: %v", err)
	}
	if len(tagsHash) != es.HashSize {
		t.Fatalf("tags_hash length = %d, want %d", len(tagsHash), es.HashSize)
	}
}

func TestSQLiteAppendWithoutTagsWritesNull(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, _ := es.NewStreamID("counter", "untagged")

	if _, err := s.Append(ctx, es.AppendParams{
		StreamID: sid,
		Events:   []es.EventData{{TypeURL: "thing.v1.Happened", Payload: []byte("1")}},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	var tags sql.NullString
	if err := s.DB().QueryRowContext(ctx, `SELECT tags FROM events`).Scan(&tags); err != nil {
		t.Fatalf("read tags: %v", err)
	}
	if tags.Valid {
		t.Fatalf("events.tags = %q, want NULL", tags.String)
	}
	var n int
	if err := s.DB().QueryRowContext(ctx, `SELECT count(*) FROM event_tags`).Scan(&n); err != nil {
		t.Fatalf("count event_tags: %v", err)
	}
	if n != 0 {
		t.Fatalf("event_tags rows = %d, want 0", n)
	}
	var tagsHash sql.Null[[]byte]
	if err := s.DB().QueryRowContext(ctx, `SELECT tags_hash FROM log_order`).Scan(&tagsHash); err != nil {
		t.Fatalf("read tags_hash: %v", err)
	}
	if tagsHash.Valid {
		t.Fatalf("tags_hash = %x, want NULL", tagsHash.V)
	}
}

func TestSQLiteAppendRejectsInvalidTags(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, _ := es.NewStreamID("counter", "invalid-tags")

	tooMany := make(map[string]string, es.MaxTagsPerEvent+1)
	for i := 0; i <= es.MaxTagsPerEvent; i++ {
		tooMany[strings.ToLower(string(rune('a'+i%26)))+string(rune('0'+i/26))] = "v"
	}
	cases := []struct {
		name  string
		batch map[string]string
		tags  map[string]string
		want  error
	}{
		{"bad key", map[string]string{"Bad": "v"}, nil, es.ErrInvalidTag},
		{"long value", map[string]string{"k": strings.Repeat("v", es.MaxTagValueBytes+1)}, nil, es.ErrInvalidTag},
		{"nul", map[string]string{"k": "a\x00b"}, nil, es.ErrInvalidTag},
		{"too many", tooMany, nil, es.ErrTooManyTags},
		{"conflict", map[string]string{"k": "batch"}, map[string]string{"k": "event"}, es.ErrConflictingTag},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.Append(ctx, es.AppendParams{
				StreamID: sid,
				Events: []es.EventData{{
					TypeURL: "thing.v1.Happened",
					Payload: []byte("x"),
					Tags:    tc.tags,
				}},
				Tags: tc.batch,
			})
			if !errors.Is(err, tc.want) {
				t.Fatalf("Append error = %v, want %v", err, tc.want)
			}
			var eventsN, tagsN int
			if err := s.DB().QueryRowContext(ctx, `SELECT count(*) FROM events`).Scan(&eventsN); err != nil {
				t.Fatalf("count events: %v", err)
			}
			if err := s.DB().QueryRowContext(ctx, `SELECT count(*) FROM event_tags`).Scan(&tagsN); err != nil {
				t.Fatalf("count event_tags: %v", err)
			}
			if eventsN != 0 || tagsN != 0 {
				t.Fatalf("rejected append wrote events=%d tags=%d, want 0/0", eventsN, tagsN)
			}
		})
	}
}

func TestSQLiteReadCorruptTagsFailsLoudly(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, _ := es.NewStreamID("counter", "corrupt-tags")

	if _, err := s.Append(ctx, es.AppendParams{
		StreamID: sid,
		Events: []es.EventData{{
			TypeURL: "thing.v1.Happened",
			Payload: []byte("x"),
			Tags:    map[string]string{"flowdoc.block": "b1"},
		}},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := s.DB().ExecContext(ctx, `UPDATE events SET tags = '{"k":1}'`); err != nil {
		t.Fatalf("corrupt tags: %v", err)
	}
	if _, err := s.ReadStream(ctx, sid, 0, 0); err == nil {
		t.Fatal("ReadStream with a non-string tag value returned no error")
	}
}

func TestSQLiteTagsDoNotChangeEventHash(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, _ := es.NewStreamID("counter", "tag-hash")

	res, err := s.Append(ctx, es.AppendParams{
		StreamID: sid,
		Events: []es.EventData{
			{TypeURL: "thing.v1.Happened", Payload: []byte("1")},
			{TypeURL: "thing.v1.Happened", Payload: []byte("2"), Tags: map[string]string{"flowdoc.block": "b1"}},
		},
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	for i, env := range res.Envelopes {
		got := es.EnvelopeHash(env)
		if !bytes.Equal(got[:], env.Hash) {
			t.Fatalf("envelope %d hash = %x, recomputed %x", i, env.Hash, got)
		}
		withoutTags := env
		withoutTags.Tags = nil
		if es.EnvelopeHash(withoutTags) != got {
			t.Fatalf("envelope %d hash changed when tags were removed", i)
		}
	}
}

func seedThreeTaggedSQLite(t *testing.T, ctx context.Context, s *sqlite.Store, sid es.StreamID) []es.Envelope {
	t.Helper()
	batches := []map[string]string{
		{"k": "range", "flowdoc.space": "s1"},
		{"k": "range", "flowdoc.space": "s1"},
		{"k": "range", "flowdoc.space": "s2"},
	}
	blocks := []string{"b1", "b2", "b1"}
	var out []es.Envelope
	for i := range batches {
		res, err := s.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: uint64(i),
			Events: []es.EventData{{
				TypeURL: "thing.v1.Happened",
				Payload: []byte{byte('a' + i)},
				Tags:    map[string]string{"flowdoc.block": blocks[i]},
			}},
			Tags: batches[i],
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		out = append(out, res.Envelopes...)
	}
	return out
}

func sqliteTagPositions(envs []es.Envelope) []uint64 {
	out := make([]uint64, len(envs))
	for i := range envs {
		out[i] = envs[i].GlobalPosition
	}
	return out
}

func TestSQLiteReadByTagsAND(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, _ := es.NewStreamID("counter", "tags-and")
	envs := seedThreeTaggedSQLite(t, ctx, s, sid)

	got, err := s.ReadByTags(ctx, es.TagQuery{Tags: map[string]string{
		"flowdoc.space": "s1",
		"flowdoc.block": "b1",
	}})
	if err != nil {
		t.Fatalf("ReadByTags: %v", err)
	}
	if len(got) != 1 || got[0].GlobalPosition != envs[0].GlobalPosition {
		t.Fatalf("ReadByTags = %v, want only position %d", sqliteTagPositions(got), envs[0].GlobalPosition)
	}
}

func TestSQLiteReadByTagsRangeAndOrder(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, _ := es.NewStreamID("counter", "tags-range")
	envs := seedThreeTaggedSQLite(t, ctx, s, sid)

	got, err := s.ReadByTags(ctx, es.TagQuery{
		Tags:         map[string]string{"k": "range"},
		FromPosition: envs[0].GlobalPosition,
		ToPosition:   envs[2].GlobalPosition,
	})
	if err != nil {
		t.Fatalf("ReadByTags: %v", err)
	}
	if want := []uint64{envs[1].GlobalPosition, envs[2].GlobalPosition}; !equalSQLitePositions(got, want) {
		t.Fatalf("ReadByTags = %v, want %v", sqliteTagPositions(got), want)
	}
	got, err = s.ReadByTags(ctx, es.TagQuery{
		Tags:       map[string]string{"k": "range"},
		ToPosition: envs[1].GlobalPosition,
	})
	if err != nil {
		t.Fatalf("ReadByTags: %v", err)
	}
	if want := []uint64{envs[0].GlobalPosition, envs[1].GlobalPosition}; !equalSQLitePositions(got, want) {
		t.Fatalf("ReadByTags = %v, want %v", sqliteTagPositions(got), want)
	}
}

func TestSQLiteReadByTagsPagination(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, _ := es.NewStreamID("counter", "tags-page")
	envs := seedThreeTaggedSQLite(t, ctx, s, sid)

	first, err := s.ReadByTags(ctx, es.TagQuery{Tags: map[string]string{"k": "range"}, Limit: 1})
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if len(first) != 1 || first[0].GlobalPosition != envs[0].GlobalPosition {
		t.Fatalf("page 1 = %v, want position %d", sqliteTagPositions(first), envs[0].GlobalPosition)
	}
	second, err := s.ReadByTags(ctx, es.TagQuery{
		Tags:         map[string]string{"k": "range"},
		FromPosition: first[0].GlobalPosition,
		Limit:        1,
	})
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	if len(second) != 1 || second[0].GlobalPosition != envs[1].GlobalPosition {
		t.Fatalf("page 2 = %v, want position %d", sqliteTagPositions(second), envs[1].GlobalPosition)
	}
}

func TestSQLiteReadByTagsLimit(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, _ := es.NewStreamID("counter", "tags-limit")
	seedThreeTaggedSQLite(t, ctx, s, sid)

	got, err := s.ReadByTags(ctx, es.TagQuery{Tags: map[string]string{"k": "range"}})
	if err != nil {
		t.Fatalf("ReadByTags: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("default limit returned %d events, want 3", len(got))
	}
	for _, limit := range []int{-1, es.MaxTagReadLimit + 1} {
		if _, err := s.ReadByTags(ctx, es.TagQuery{Tags: map[string]string{"k": "range"}, Limit: limit}); !errors.Is(err, es.ErrInvalidTagQuery) {
			t.Fatalf("Limit=%d error = %v, want ErrInvalidTagQuery", limit, err)
		}
	}
}

func TestSQLiteReadByTagsRejectsEmptyQuery(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	if _, err := s.ReadByTags(ctx, es.TagQuery{}); !errors.Is(err, es.ErrInvalidTagQuery) {
		t.Fatalf("empty query error = %v, want ErrInvalidTagQuery", err)
	}
}

func TestSQLiteReadByTagsRejectsInvertedRange(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	if _, err := s.ReadByTags(ctx, es.TagQuery{
		Tags:         map[string]string{"k": "v"},
		FromPosition: 5,
		ToPosition:   4,
	}); !errors.Is(err, es.ErrInvalidTagQuery) {
		t.Fatalf("inverted range error = %v, want ErrInvalidTagQuery", err)
	}
}

func TestSQLiteReadByTagsAsOf(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, _ := es.NewStreamID("counter", "tags-asof")
	envs := seedThreeTaggedSQLite(t, ctx, s, sid)

	got, err := s.ReadByTags(ctx, es.TagQuery{
		Tags:       map[string]string{"k": "range"},
		ToPosition: envs[1].GlobalPosition,
	})
	if err != nil {
		t.Fatalf("ReadByTags: %v", err)
	}
	if want := []uint64{envs[0].GlobalPosition, envs[1].GlobalPosition}; !equalSQLitePositions(got, want) {
		t.Fatalf("as-of read = %v, want %v", sqliteTagPositions(got), want)
	}
}

func equalSQLitePositions(envs []es.Envelope, want []uint64) bool {
	got := sqliteTagPositions(envs)
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
