package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/laenenai/es-lite/es"
)

var (
	_ es.TagMaintainer = (*Store)(nil)
	_ es.TagAttester   = (*Store)(nil)
)

// RebuildTags replaces the live event_tags projection by re-projecting every
// tagged event. It is the restore path for SQLite. One transaction: the
// non-string guard, the delete, the insert and the final count see one
// snapshot, so a failure rolls the whole rebuild back.
func (s *Store) RebuildTags(ctx context.Context) (es.TagsRebuildReport, error) {
	var zero es.TagsRebuildReport
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return zero, fmt.Errorf("sqlite: rebuild tags: begin: %w", err)
	}
	defer tx.Rollback()

	bad, err := countRows(ctx, tx, `
		SELECT count(*) FROM events e, json_each(e.tags) AS kv
		WHERE e.tags IS NOT NULL AND kv.type <> 'text'`)
	if err != nil {
		return zero, fmt.Errorf("sqlite: rebuild tags: check values: %w", err)
	}
	if bad > 0 {
		return zero, fmt.Errorf("sqlite: rebuild tags: %d stored tag values are not strings", bad)
	}

	events, err := countRows(ctx, tx, `SELECT count(*) FROM events WHERE tags IS NOT NULL`)
	if err != nil {
		return zero, fmt.Errorf("sqlite: rebuild tags: count tagged events: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM event_tags`); err != nil {
		return zero, fmt.Errorf("sqlite: rebuild tags: clear index: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO event_tags (tag_key, tag_value, global_position)
		SELECT kv.key, kv.value, e.global_position
		FROM events e, json_each(e.tags) AS kv
		WHERE e.tags IS NOT NULL`); err != nil {
		return zero, fmt.Errorf("sqlite: rebuild tags: project: %w", err)
	}

	tags, err := countRows(ctx, tx, `SELECT count(*) FROM event_tags`)
	if err != nil {
		return zero, fmt.Errorf("sqlite: rebuild tags: count tags: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return zero, fmt.Errorf("sqlite: rebuild tags: commit: %w", err)
	}
	return es.TagsRebuildReport{Events: events, Tags: tags}, nil
}

// VerifyTags rebuilds the projection into a temporary table and reports the
// first (key, value, position) where it and the live event_tags disagree.
// It refuses a stored non-string tag value rather than stringifying it.
func (s *Store) VerifyTags(ctx context.Context) (es.TagsVerifyReport, error) {
	var zero es.TagsVerifyReport
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return zero, fmt.Errorf("sqlite: verify tags: begin: %w", err)
	}
	defer tx.Rollback()

	bad, err := countRows(ctx, tx, `
		SELECT count(*) FROM events e, json_each(e.tags) AS kv
		WHERE e.tags IS NOT NULL AND kv.type <> 'text'`)
	if err != nil {
		return zero, fmt.Errorf("sqlite: verify tags: check values: %w", err)
	}
	if bad > 0 {
		return zero, fmt.Errorf("sqlite: verify tags: %d stored tag values are not strings", bad)
	}

	events, err := countRows(ctx, tx, `SELECT count(*) FROM events WHERE tags IS NOT NULL`)
	if err != nil {
		return zero, fmt.Errorf("sqlite: verify tags: count tagged events: %w", err)
	}

	// A reused temp table must never leak a previous run's rows.
	if _, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS temp.tags_rebuild`); err != nil {
		return zero, fmt.Errorf("sqlite: verify tags: drop temp index: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		CREATE TEMP TABLE tags_rebuild (
		    tag_key         TEXT    NOT NULL,
		    tag_value       TEXT    NOT NULL,
		    global_position INTEGER NOT NULL,
		    PRIMARY KEY (tag_key, tag_value, global_position)
		)`); err != nil {
		return zero, fmt.Errorf("sqlite: verify tags: create temp index: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO tags_rebuild (tag_key, tag_value, global_position)
		SELECT kv.key, kv.value, e.global_position
		FROM events e, json_each(e.tags) AS kv
		WHERE e.tags IS NOT NULL`); err != nil {
		return zero, fmt.Errorf("sqlite: verify tags: project temp: %w", err)
	}

	tags, err := countRows(ctx, tx, `SELECT count(*) FROM tags_rebuild`)
	if err != nil {
		return zero, fmt.Errorf("sqlite: verify tags: count rebuilt tags: %w", err)
	}

	rep := es.TagsVerifyReport{OK: true, Events: events, Tags: tags}
	var (
		key   string
		value string
		pos   int64
	)
	err = tx.QueryRowContext(ctx, `
		SELECT * FROM (
		    SELECT tag_key, tag_value, global_position FROM event_tags
		    EXCEPT
		    SELECT tag_key, tag_value, global_position FROM tags_rebuild
		)
		UNION ALL
		SELECT * FROM (
		    SELECT tag_key, tag_value, global_position FROM tags_rebuild
		    EXCEPT
		    SELECT tag_key, tag_value, global_position FROM event_tags
		)
		LIMIT 1`).Scan(&key, &value, &pos)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// The two directions agree.
	case err != nil:
		return zero, fmt.Errorf("sqlite: verify tags: diff: %w", err)
	default:
		rep.OK = false
		rep.FirstDivergence = fmt.Sprintf("key=%s value=%q position=%d", key, value, pos)
	}

	if err := tx.Commit(); err != nil {
		return zero, fmt.Errorf("sqlite: verify tags: commit: %w", err)
	}
	return rep, nil
}

// VerifyTagHashes recomputes each covered batch's tags hash from the events
// and compares it with log_order.tags_hash. It reads events.tags, never
// event_tags, so a changed tag cannot be laundered through the index.
// RowsChecked counts tagged batches; an untagged batch is checked for "no
// entries" but is not counted.
func (s *Store) VerifyTagHashes(ctx context.Context, upto uint64) (*es.TagsHashVerifyReport, error) {
	rows, err := s.readLogOrderRows(ctx, upto)
	if err != nil {
		return nil, err
	}

	var checked uint64
	for i := range rows {
		r := &rows[i]
		entries, err := readSQLiteTagEntries(ctx, s.db, r.Stream, r.From, r.To)
		if err != nil {
			return nil, err
		}
		got := es.TagsHash(entries)

		if r.TagsHash == nil {
			if len(entries) == 0 {
				continue
			}
			return &es.TagsHashVerifyReport{
				OK:          false,
				RowsChecked: checked + 1,
				Mismatch: &es.TagRowMismatch{
					From:             r.From,
					ExpectedTagsHash: nil,
					ActualTagsHash:   got[:],
				},
			}, nil
		}

		checked++
		if !bytes.Equal(got[:], r.TagsHash) {
			return &es.TagsHashVerifyReport{
				OK:          false,
				RowsChecked: checked,
				Mismatch: &es.TagRowMismatch{
					From:             r.From,
					ExpectedTagsHash: r.TagsHash,
					ActualTagsHash:   got[:],
				},
			}, nil
		}
	}
	return &es.TagsHashVerifyReport{OK: true, RowsChecked: checked}, nil
}

// readSQLiteTagEntries expands one batch's events into the (position, key,
// value) entries TagsHash attests. The query is scoped by stream, so
// interleaved positions from other streams cannot leak into the batch.
func readSQLiteTagEntries(ctx context.Context, db *sql.DB, stream string, from, to uint64) ([]es.TagEntry, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT global_position, tags FROM events
		WHERE stream_id = ?
		  AND global_position BETWEEN ? AND ?
		  AND tags IS NOT NULL
		ORDER BY global_position`,
		stream, int64(from), int64(to))
	if err != nil {
		return nil, fmt.Errorf("sqlite: verify tag hashes: read events: %w", err)
	}
	defer rows.Close()

	var entries []es.TagEntry
	for rows.Next() {
		var (
			pos uint64
			raw string
		)
		if err := rows.Scan(&pos, &raw); err != nil {
			return nil, fmt.Errorf("sqlite: verify tag hashes: scan event: %w", err)
		}
		tags, err := es.DecodeTags([]byte(raw))
		if err != nil {
			return nil, fmt.Errorf("sqlite: verify tag hashes: stream %s position %d: %w", stream, pos, err)
		}
		for k, v := range tags {
			entries = append(entries, es.TagEntry{Position: pos, Key: k, Value: v})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: verify tag hashes: rows: %w", err)
	}
	return entries, nil
}
