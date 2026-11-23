package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/laenenai/es-lite/es"
)

var (
	_ es.TagMaintainer = (*Workspace)(nil)
	_ es.TagAttester   = (*Store)(nil)
)

// storedTagsPage bounds the validation scan's working set. The rebuild and
// verify already read every tagged event; this only keeps that pre-pass from
// materializing the whole workspace at once.
const storedTagsPage = 1000

// validateStoredTags pages the workspace's tagged events and decodes each
// with the strict stored-tag decoder: the value must be a JSON object whose
// keys and values pass the append vocabulary and whose count is within the
// cap. It runs before RebuildTags deletes anything and before VerifyTags
// builds its temp table, so corruption is refused rather than stringified or
// certified. The scan is keyset-paged by global_position.
func validateStoredTags(ctx context.Context, tx pgx.Tx, workspace string) error {
	var after uint64
	for {
		rows, err := tx.Query(ctx, `
			SELECT global_position, stream_id, tags FROM events
			WHERE workspace_id = $1 AND tags IS NOT NULL
			  AND global_position > $2
			ORDER BY global_position
			LIMIT $3`, workspace, int64(after), storedTagsPage)
		if err != nil {
			return fmt.Errorf("read stored tags: %w", err)
		}
		n := 0
		for rows.Next() {
			var (
				pos    uint64
				stream string
				raw    []byte
			)
			if err := rows.Scan(&pos, &stream, &raw); err != nil {
				rows.Close()
				return fmt.Errorf("scan stored tags: %w", err)
			}
			if _, err := es.DecodeStoredTags(raw); err != nil {
				rows.Close()
				return fmt.Errorf("workspace %s stream %s position %d: %w", workspace, stream, pos, err)
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
// tagged event in the workspace. It is the restore path: load events, run
// this, and the tag index is back. One Repeatable Read transaction: the guard,
// the delete, the insert and the final count see one snapshot, so a decode
// failure rolls the whole rebuild back and leaves the live index as it was.
//
// Every stored tags value is decoded and validated against the append
// vocabulary before the delete: a non-object, an invalid key or value, or an
// over-cap set is refused. jsonb_each_text would silently stringify a
// non-string member; this path must not turn corruption into a plausible tag
// row.
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

	if err := validateStoredTags(ctx, tx, w.ws); err != nil {
		return zero, fmt.Errorf("postgres: rebuild tags: %w", err)
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

	// Mirror RebuildTags' fail-loud guard: jsonb_each_text would stringify a
	// non-string value, turning corruption into a plausible divergence, and
	// an invalid key or over-cap set would be certified as a valid
	// projection.
	if err := validateStoredTags(ctx, tx, w.ws); err != nil {
		return zero, fmt.Errorf("postgres: verify tags: %w", err)
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
//
// One LEFT JOIN returns every covered batch and its tagged events, ordered by
// from_position, so the audit is one round trip rather than one query per
// batch. An untagged batch still yields one row with NULL event columns —
// exactly the "expected no entries" case a NULL tags_hash asserts.
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

	rows, err := tx.Query(ctx, `
		SELECT lo.from_position, lo.to_position, lo.workspace_id, lo.stream_id, lo.tags_hash,
		       e.global_position, e.tags
		FROM log_order lo
		LEFT JOIN events e
		  ON e.workspace_id = lo.workspace_id
		 AND e.stream_id = lo.stream_id
		 AND e.global_position BETWEEN lo.from_position AND lo.to_position
		 AND e.tags IS NOT NULL
		WHERE ($1::bigint = 0 OR lo.to_position <= $1)
		ORDER BY lo.from_position, e.global_position`, int64(bound))
	if err != nil {
		return nil, fmt.Errorf("postgres: verify tag hashes: read batches: %w", err)
	}
	defer rows.Close()

	type tagBatch struct {
		from, to          uint64
		workspace, stream string
		stored            []byte
		entries           []es.TagEntry
	}

	var (
		checked uint64
		cur     *tagBatch
	)
	// finish compares one completed batch and reports whether it was the
	// first mismatch. A NULL stored hash must have no entries; a stored hash
	// must match the recomputed one.
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
			from, to          uint64
			workspace, stream string
			stored            []byte
			pos               pgtype.Int8
			raw               []byte
		)
		if err := rows.Scan(&from, &to, &workspace, &stream, &stored, &pos, &raw); err != nil {
			return nil, fmt.Errorf("postgres: verify tag hashes: scan batch: %w", err)
		}
		if cur == nil || cur.from != from {
			if cur != nil {
				if rep, mismatch := finish(cur); mismatch {
					return rep, nil
				}
			}
			cur = &tagBatch{
				from: from, to: to, workspace: workspace, stream: stream,
				stored: append([]byte(nil), stored...),
			}
		}
		if !pos.Valid {
			continue
		}
		tags, err := es.DecodeStoredTags(raw)
		if err != nil {
			return nil, fmt.Errorf(
				"postgres: verify tag hashes: workspace %s stream %s position %d: %w",
				workspace, stream, pos.Int64, err)
		}
		for k, v := range tags {
			cur.entries = append(cur.entries, es.TagEntry{
				Position: uint64(pos.Int64), Key: k, Value: v,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: verify tag hashes: rows: %w", err)
	}
	if cur != nil {
		if rep, mismatch := finish(cur); mismatch {
			return rep, nil
		}
	}
	return &es.TagsHashVerifyReport{OK: true, RowsChecked: checked}, nil
}
