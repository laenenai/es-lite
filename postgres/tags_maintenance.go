package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/laenenai/es-lite/es"
)

var (
	_ es.TagMaintainer = (*Workspace)(nil)
	_ es.TagAttester   = (*Store)(nil)
)

// RebuildTags replaces the live event_tags projection by re-projecting every
// tagged event in the workspace. It is the restore path: load events, run
// this, and the tag index is back. One Repeatable Read transaction: the guard,
// the delete, the insert and the final count see one snapshot, so a decode
// failure rolls the whole rebuild back and leaves the live index as it was.
//
// A stored tags value that is an object with a non-string member is refused
// before the delete. jsonb_each_text would silently stringify it; this path
// must not turn corruption into a plausible tag row.
func (w *Workspace) RebuildTags(ctx context.Context) (es.TagsRebuildReport, error) {
	var zero es.TagsRebuildReport
	tx, err := w.store.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return zero, fmt.Errorf("postgres: rebuild tags: begin: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := setWorkspace(ctx, tx, w.ws); err != nil {
		return zero, fmt.Errorf("postgres: rebuild tags: set workspace: %w", err)
	}

	bad, err := countRows(ctx, tx, `
		SELECT count(*) FROM events e, jsonb_each(e.tags) kv
		WHERE e.workspace_id = $1 AND e.tags IS NOT NULL
		  AND jsonb_typeof(kv.value) <> 'string'`, w.ws)
	if err != nil {
		return zero, fmt.Errorf("postgres: rebuild tags: check values: %w", err)
	}
	if bad > 0 {
		return zero, fmt.Errorf("postgres: rebuild tags: %d stored tag values are not strings", bad)
	}

	events, err := countRows(ctx, tx,
		`SELECT count(*) FROM events WHERE workspace_id = $1 AND tags IS NOT NULL`, w.ws)
	if err != nil {
		return zero, fmt.Errorf("postgres: rebuild tags: count tagged events: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`DELETE FROM event_tags WHERE workspace_id = $1`, w.ws); err != nil {
		return zero, fmt.Errorf("postgres: rebuild tags: clear index: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO event_tags (workspace_id, tag_key, tag_value, global_position)
		SELECT e.workspace_id, kv.key, kv.value, e.global_position
		FROM events e, jsonb_each_text(e.tags) AS kv
		WHERE e.workspace_id = $1 AND e.tags IS NOT NULL`, w.ws); err != nil {
		return zero, fmt.Errorf("postgres: rebuild tags: project: %w", err)
	}

	tags, err := countRows(ctx, tx,
		`SELECT count(*) FROM event_tags WHERE workspace_id = $1`, w.ws)
	if err != nil {
		return zero, fmt.Errorf("postgres: rebuild tags: count tags: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return zero, fmt.Errorf("postgres: rebuild tags: commit: %w", err)
	}
	return es.TagsRebuildReport{Events: events, Tags: tags}, nil
}

// VerifyTags rebuilds the projection into a temporary table and reports the
// first (key, value, position) where it and the live event_tags disagree.
// It never modifies the live index; the temp table drops with the transaction.
// One Repeatable Read snapshot keeps a concurrent append from manufacturing a
// false divergence.
func (w *Workspace) VerifyTags(ctx context.Context) (es.TagsVerifyReport, error) {
	var zero es.TagsVerifyReport
	tx, err := w.store.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return zero, fmt.Errorf("postgres: verify tags: begin: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := setWorkspace(ctx, tx, w.ws); err != nil {
		return zero, fmt.Errorf("postgres: verify tags: set workspace: %w", err)
	}

	// Mirror RebuildTags' fail-loud guard: jsonb_each_text would stringify
	// a non-string value, turning corruption into a plausible divergence.
	bad, err := countRows(ctx, tx, `
		SELECT count(*) FROM events e, jsonb_each(e.tags) kv
		WHERE e.workspace_id = $1 AND e.tags IS NOT NULL
		  AND jsonb_typeof(kv.value) <> 'string'`, w.ws)
	if err != nil {
		return zero, fmt.Errorf("postgres: verify tags: check values: %w", err)
	}
	if bad > 0 {
		return zero, fmt.Errorf("postgres: verify tags: %d stored tag values are not strings", bad)
	}

	events, err := countRows(ctx, tx,
		`SELECT count(*) FROM events WHERE workspace_id = $1 AND tags IS NOT NULL`, w.ws)
	if err != nil {
		return zero, fmt.Errorf("postgres: verify tags: count tagged events: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`CREATE TEMP TABLE tags_rebuild (LIKE event_tags INCLUDING ALL) ON COMMIT DROP`); err != nil {
		return zero, fmt.Errorf("postgres: verify tags: create temp index: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO tags_rebuild (workspace_id, tag_key, tag_value, global_position)
		SELECT e.workspace_id, kv.key, kv.value, e.global_position
		FROM events e, jsonb_each_text(e.tags) AS kv
		WHERE e.workspace_id = $1 AND e.tags IS NOT NULL`, w.ws); err != nil {
		return zero, fmt.Errorf("postgres: verify tags: project temp: %w", err)
	}

	tags, err := countRows(ctx, tx,
		`SELECT count(*) FROM tags_rebuild WHERE workspace_id = $1`, w.ws)
	if err != nil {
		return zero, fmt.Errorf("postgres: verify tags: count rebuilt tags: %w", err)
	}

	rep := es.TagsVerifyReport{OK: true, Events: events, Tags: tags}
	var (
		key   string
		value string
		pos   int64
	)
	err = tx.QueryRow(ctx, `
		(SELECT tag_key, tag_value, global_position FROM event_tags WHERE workspace_id = $1
		 EXCEPT
		 SELECT tag_key, tag_value, global_position FROM tags_rebuild WHERE workspace_id = $2)
		UNION ALL
		(SELECT tag_key, tag_value, global_position FROM tags_rebuild WHERE workspace_id = $3
		 EXCEPT
		 SELECT tag_key, tag_value, global_position FROM event_tags WHERE workspace_id = $4)
		LIMIT 1`,
		w.ws, w.ws, w.ws, w.ws,
	).Scan(&key, &value, &pos)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// The two directions agree.
	case err != nil:
		return zero, fmt.Errorf("postgres: verify tags: diff: %w", err)
	default:
		rep.OK = false
		rep.FirstDivergence = fmt.Sprintf("key=%s value=%q position=%d", key, value, pos)
	}

	if err := tx.Commit(ctx); err != nil {
		return zero, fmt.Errorf("postgres: verify tags: commit: %w", err)
	}
	return rep, nil
}

// VerifyTagHashes recomputes each covered batch's tags hash from the events
// and compares it with log_order.tags_hash. It is the attestation audit, not
// a projection check: it reads events.tags, never event_tags, so a DB writer
// cannot launder a changed tag through the index. RowsChecked counts tagged
// batches; an untagged batch (NULL tags_hash) is checked for "no entries" but
// is not counted.
func (s *Store) VerifyTagHashes(ctx context.Context, upto uint64) (*es.TagsHashVerifyReport, error) {
	if err := s.checkRelayRole(ctx); err != nil {
		return nil, err
	}
	bound, err := s.settledBound(ctx, upto)
	if err != nil {
		return nil, err
	}
	tx, err := s.bypassTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := readLogOrderRows(ctx, tx, bound)
	if err != nil {
		return nil, err
	}

	var checked uint64
	for i := range rows {
		r := &rows[i]
		entries, err := readTagEntries(ctx, tx, r.Workspace, r.Stream, r.From, r.To)
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

// readTagEntries pages one batch's events and expands their tags into the
// (position, key, value) entries TagsHash attests. The query is scoped by
// workspace and stream, so concurrently interleaved positions from other
// streams cannot leak into the batch.
func readTagEntries(ctx context.Context, tx pgx.Tx, workspace, stream string, from, to uint64) ([]es.TagEntry, error) {
	rows, err := tx.Query(ctx, `
		SELECT global_position, tags FROM events
		WHERE workspace_id = $1 AND stream_id = $2
		  AND global_position BETWEEN $3 AND $4
		  AND tags IS NOT NULL
		ORDER BY global_position`,
		workspace, stream, int64(from), int64(to))
	if err != nil {
		return nil, fmt.Errorf("postgres: verify tag hashes: read events: %w", err)
	}
	defer rows.Close()

	var entries []es.TagEntry
	for rows.Next() {
		var (
			pos uint64
			raw []byte
		)
		if err := rows.Scan(&pos, &raw); err != nil {
			return nil, fmt.Errorf("postgres: verify tag hashes: scan event: %w", err)
		}
		tags, err := es.DecodeTags(raw)
		if err != nil {
			return nil, fmt.Errorf("postgres: verify tag hashes: workspace %s stream %s position %d: %w",
				workspace, stream, pos, err)
		}
		for k, v := range tags {
			entries = append(entries, es.TagEntry{Position: pos, Key: k, Value: v})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: verify tag hashes: rows: %w", err)
	}
	return entries, nil
}
