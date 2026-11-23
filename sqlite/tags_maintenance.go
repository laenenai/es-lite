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

// storedTagsPage bounds the validation scan's working set. The rebuild and
// verify already read every tagged event; this only keeps that pre-pass from
// materializing the whole store at once.
const storedTagsPage = 1000

// validateStoredTags pages the tagged events and decodes each with the strict
// stored-tag decoder: the value must be a JSON object whose keys and values
// pass the append vocabulary and whose count is within the cap. SQLite's
// json_each would happily expand a string array into key "0", "1"; the
// decoder refuses it. It runs before RebuildTags deletes anything and before
// VerifyTags builds its temp table.
func validateStoredTags(ctx context.Context, tx *sql.Tx) error {
	var after int64
	for {
		rows, err := tx.QueryContext(ctx, `
			SELECT global_position, stream_id, tags FROM events
			WHERE tags IS NOT NULL AND global_position > ?
			ORDER BY global_position
			LIMIT ?`, after, storedTagsPage)
		if err != nil {
			return fmt.Errorf("read stored tags: %w", err)
		}
		n := 0
		for rows.Next() {
			var (
				pos    int64
				stream string
				raw    string
			)
			if err := rows.Scan(&pos, &stream, &raw); err != nil {
				rows.Close()
				return fmt.Errorf("scan stored tags: %w", err)
			}
			if _, err := es.DecodeStoredTags([]byte(raw)); err != nil {
				rows.Close()
				return fmt.Errorf("stream %s position %d: %w", stream, pos, err)
			}
			after = pos
			n++
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return fmt.Errorf("stored tags rows: %w", err)
		}
		if n < storedTagsPage {
			return nil
		}
	}
}

// RebuildTags replaces the live event_tags projection by re-projecting every
// tagged event. It is the restore path for SQLite. One transaction: the
// stored-tag guard, the delete, the insert and the final count see one
// snapshot, so a failure rolls the whole rebuild back.
func (s *Store) RebuildTags(ctx context.Context) (es.TagsRebuildReport, error) {
	var zero es.TagsRebuildReport
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return zero, fmt.Errorf("sqlite: rebuild tags: begin: %w", err)
	}
	defer tx.Rollback()

	if err := validateStoredTags(ctx, tx); err != nil {
		return zero, fmt.Errorf("sqlite: rebuild tags: %w", err)
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

	if err := validateStoredTags(ctx, tx); err != nil {
		return zero, fmt.Errorf("sqlite: verify tags: %w", err)
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
//
// One LEFT JOIN returns every covered batch and its tagged events, ordered by
// from_position, so the audit is one round trip rather than one query per
// batch. An untagged batch still yields one row with NULL event columns —
// exactly the "expected no entries" case a NULL tags_hash asserts.
func (s *Store) VerifyTagHashes(ctx context.Context, upto uint64) (*es.TagsHashVerifyReport, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT lo.from_position, lo.to_position, lo.stream_id, lo.tags_hash,
		       e.global_position, e.tags
		FROM log_order lo
		LEFT JOIN events e
		  ON e.stream_id = lo.stream_id
		 AND e.global_position BETWEEN lo.from_position AND lo.to_position
		 AND e.tags IS NOT NULL
		WHERE (? = 0 OR lo.to_position <= ?)
		ORDER BY lo.from_position, e.global_position`, int64(upto), int64(upto))
	if err != nil {
		return nil, fmt.Errorf("sqlite: verify tag hashes: read batches: %w", err)
	}
	defer rows.Close()

	type tagBatch struct {
		from, to uint64
		stream   string
		stored   []byte
		entries  []es.TagEntry
	}

	var (
		checked uint64
		cur     *tagBatch
	)
	finish := func(b *tagBatch) (*es.TagsHashVerifyReport, bool) {
		got := es.TagsHash(b.entries)
		if b.stored == nil {
			if len(b.entries) == 0 {
				return nil, false
			}
			return &es.TagsHashVerifyReport{
				OK:          false,
				RowsChecked: checked,
				Mismatch: &es.TagRowMismatch{
					From:             b.from,
					ExpectedTagsHash: nil,
					ActualTagsHash:   got[:],
				},
			}, true
		}
		checked++
		if !bytes.Equal(got[:], b.stored) {
			return &es.TagsHashVerifyReport{
				OK:          false,
				RowsChecked: checked,
				Mismatch: &es.TagRowMismatch{
					From:             b.from,
					ExpectedTagsHash: b.stored,
					ActualTagsHash:   got[:],
				},
			}, true
		}
		return nil, false
	}

	for rows.Next() {
		var (
			from, to uint64
			stream   string
			stored   []byte
			pos      sql.NullInt64
			raw      sql.NullString
		)
		if err := rows.Scan(&from, &to, &stream, &stored, &pos, &raw); err != nil {
			return nil, fmt.Errorf("sqlite: verify tag hashes: scan batch: %w", err)
		}
		if cur == nil || cur.from != from {
			if cur != nil {
				if rep, mismatch := finish(cur); mismatch {
					return rep, nil
				}
			}
			cur = &tagBatch{
				from: from, to: to, stream: stream,
				stored: append([]byte(nil), stored...),
			}
		}
		if !pos.Valid {
			continue
		}
		tags, err := es.DecodeStoredTags([]byte(raw.String))
		if err != nil {
			return nil, fmt.Errorf(
				"sqlite: verify tag hashes: stream %s position %d: %w",
				stream, pos.Int64, err)
		}
		for k, v := range tags {
			cur.entries = append(cur.entries, es.TagEntry{
				Position: uint64(pos.Int64), Key: k, Value: v,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: verify tag hashes: rows: %w", err)
	}
	if cur != nil {
		if rep, mismatch := finish(cur); mismatch {
			return rep, nil
		}
	}
	return &es.TagsHashVerifyReport{OK: true, RowsChecked: checked}, nil
}
