package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/laenenai/es-lite/es"
)

var _ es.Anchorer = (*Store)(nil)

// ChainedSince returns when migration 0002 (the hash chain) was applied, for
// es.VerifyOptions.ChainedSince. The zero time means it has not run.
func (s *Store) ChainedSince(ctx context.Context) (time.Time, error) {
	var applied time.Time
	err := s.pool.QueryRow(ctx, `SELECT applied_at FROM schema_migrations WHERE version = 2`).Scan(&applied)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	return applied.UTC(), nil
}

// bypassTx opens a transaction with RLS bypassed — the admin/relay path, as
// Drain uses (ADR 0004). Anchors span every workspace by construction.
func (s *Store) bypassTx(ctx context.Context) (pgx.Tx, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.bypass_rls', 'on', true)`); err != nil {
		tx.Rollback(ctx)
		return nil, fmt.Errorf("set bypass: %w", err)
	}
	return tx, nil
}

// Anchor implements es.Anchorer: it computes the root over every stream's
// head at or below upto (0 = the log head) across all workspaces, records
// the row and returns it. Runs under app.bypass_rls.
func (s *Store) Anchor(ctx context.Context, upto uint64) (es.Anchor, error) {
	tx, err := s.bypassTx(ctx)
	if err != nil {
		return es.Anchor{}, err
	}
	defer tx.Rollback(ctx)

	if upto == 0 {
		if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(global_position), 0) FROM events`).Scan(&upto); err != nil {
			return es.Anchor{}, fmt.Errorf("read log head: %w", err)
		}
	}
	heads, err := heads(ctx, tx, upto)
	if err != nil {
		return es.Anchor{}, err
	}
	root := es.AnchorRoot(heads)
	a := es.Anchor{
		ID:                 newV7().String(),
		CreatedAt:          es.TruncateMicros(time.Now()),
		UptoGlobalPosition: upto,
		StreamCount:        uint64(len(heads)),
		RootHash:           root[:],
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO anchors (anchor_id, created_at, upto_global_position, stream_count, root_hash) VALUES ($1,$2,$3,$4,$5)`,
		a.ID, a.CreatedAt, a.UptoGlobalPosition, a.StreamCount, a.RootHash,
	); err != nil {
		return es.Anchor{}, fmt.Errorf("insert anchor: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return es.Anchor{}, fmt.Errorf("commit: %w", err)
	}
	return a, nil
}

// ListAnchors implements es.Anchorer.
func (s *Store) ListAnchors(ctx context.Context) ([]es.Anchor, error) {
	tx, err := s.bypassTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx,
		`SELECT anchor_id, created_at, upto_global_position, stream_count, root_hash FROM anchors ORDER BY upto_global_position, created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []es.Anchor
	for rows.Next() {
		var a es.Anchor
		if err := rows.Scan(&a.ID, &a.CreatedAt, &a.UptoGlobalPosition, &a.StreamCount, &a.RootHash); err != nil {
			return nil, err
		}
		a.CreatedAt = a.CreatedAt.UTC()
		out = append(out, a)
	}
	return out, rows.Err()
}

// CheckAnchor implements es.Anchorer: recomputes the root at the anchor's
// position and compares.
func (s *Store) CheckAnchor(ctx context.Context, a es.Anchor) error {
	tx, err := s.bypassTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	hs, err := heads(ctx, tx, a.UptoGlobalPosition)
	if err != nil {
		return err
	}
	root := es.AnchorRoot(hs)
	if uint64(len(hs)) != a.StreamCount || !bytes.Equal(root[:], a.RootHash) {
		return fmt.Errorf("%w: anchor %s at global_position %d (streams %d, recomputed %d)",
			es.ErrAnchorMismatch, a.ID, a.UptoGlobalPosition, a.StreamCount, len(hs))
	}
	return nil
}

// ListStreams implements es.Anchorer: every (workspace, stream) in the
// store, under app.bypass_rls.
func (s *Store) ListStreams(ctx context.Context) ([]es.StreamRef, error) {
	tx, err := s.bypassTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT DISTINCT workspace_id, stream_id FROM events ORDER BY workspace_id, stream_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []es.StreamRef
	for rows.Next() {
		var ws, canonical string
		if err := rows.Scan(&ws, &canonical); err != nil {
			return nil, err
		}
		sid, err := es.ParseCanonical(canonical)
		if err != nil {
			return nil, err
		}
		out = append(out, es.StreamRef{Workspace: ws, StreamID: sid})
	}
	return out, rows.Err()
}

// heads returns each (workspace, stream)'s highest version at or below upto
// with its hash.
func heads(ctx context.Context, tx pgx.Tx, upto uint64) ([]es.StreamHead, error) {
	rows, err := tx.Query(ctx,
		`SELECT e.workspace_id, e.stream_id, e.version, e.hash
		   FROM events e
		   JOIN (SELECT workspace_id, stream_id, MAX(version) AS v
		           FROM events WHERE global_position <= $1
		          GROUP BY workspace_id, stream_id) m
		     ON m.workspace_id = e.workspace_id AND m.stream_id = e.stream_id AND m.v = e.version
		  ORDER BY e.workspace_id, e.stream_id`, upto)
	if err != nil {
		return nil, fmt.Errorf("read stream heads: %w", err)
	}
	defer rows.Close()
	var out []es.StreamHead
	for rows.Next() {
		var (
			h         es.StreamHead
			canonical string
		)
		if err := rows.Scan(&h.Workspace, &canonical, &h.Version, &h.Hash); err != nil {
			return nil, err
		}
		if h.StreamID, err = es.ParseCanonical(canonical); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}
