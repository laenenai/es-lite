package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/laenenai/es-lite/es"
)

var _ es.Anchorer = (*Store)(nil)

// ChainedSince returns when migration 0002 (the hash chain) was applied, for
// es.VerifyOptions.ChainedSince: any unhashed row recorded after it is a
// break. The zero time means the migration has not run.
func (s *Store) ChainedSince(ctx context.Context) (time.Time, error) {
	var applied string
	err := s.db.QueryRowContext(ctx, `SELECT applied_at FROM schema_migrations WHERE version = 2`).Scan(&applied)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	// schema_migrations.applied_at is strftime('%Y-%m-%dT%H:%M:%fZ') — ms
	// precision, not the fixed-width ns layout events use.
	t, err := time.Parse("2006-01-02T15:04:05.000Z", applied)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse applied_at %q: %w", applied, err)
	}
	return t, nil
}

// Anchor implements es.Anchorer: it computes the root over every stream's
// head at or below upto (0 = the log head), records the row and returns it.
func (s *Store) Anchor(ctx context.Context, upto uint64) (es.Anchor, error) {
	if upto == 0 {
		if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(global_position), 0) FROM events`).Scan(&upto); err != nil {
			return es.Anchor{}, fmt.Errorf("read log head: %w", err)
		}
	}
	heads, err := s.heads(ctx, upto)
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
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO anchors (anchor_id, created_at, upto_global_position, stream_count, root_hash) VALUES (?,?,?,?,?)`,
		a.ID, formatTS(a.CreatedAt), a.UptoGlobalPosition, a.StreamCount, a.RootHash,
	); err != nil {
		return es.Anchor{}, fmt.Errorf("insert anchor: %w", err)
	}
	return a, nil
}

// ListAnchors implements es.Anchorer.
func (s *Store) ListAnchors(ctx context.Context) ([]es.Anchor, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT anchor_id, created_at, upto_global_position, stream_count, root_hash FROM anchors ORDER BY upto_global_position, created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []es.Anchor
	for rows.Next() {
		var (
			a       es.Anchor
			created string
		)
		if err := rows.Scan(&a.ID, &created, &a.UptoGlobalPosition, &a.StreamCount, &a.RootHash); err != nil {
			return nil, err
		}
		if a.CreatedAt, err = parseTS(created); err != nil {
			return nil, fmt.Errorf("parse created_at %q: %w", created, err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// CheckAnchor implements es.Anchorer: recomputes the root at the anchor's
// position and compares.
func (s *Store) CheckAnchor(ctx context.Context, a es.Anchor) error {
	heads, err := s.heads(ctx, a.UptoGlobalPosition)
	if err != nil {
		return err
	}
	root := es.AnchorRoot(heads)
	if uint64(len(heads)) != a.StreamCount || !bytes.Equal(root[:], a.RootHash) {
		return fmt.Errorf("%w: anchor %s at global_position %d (streams %d, recomputed %d)",
			es.ErrAnchorMismatch, a.ID, a.UptoGlobalPosition, a.StreamCount, len(heads))
	}
	return nil
}

// ListStreams implements es.Anchorer. SQLite is single-workspace, so the
// Workspace of every ref is empty.
func (s *Store) ListStreams(ctx context.Context) ([]es.StreamRef, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT stream_id FROM events ORDER BY stream_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []es.StreamRef
	for rows.Next() {
		var canonical string
		if err := rows.Scan(&canonical); err != nil {
			return nil, err
		}
		sid, err := es.ParseCanonical(canonical)
		if err != nil {
			return nil, err
		}
		out = append(out, es.StreamRef{StreamID: sid})
	}
	return out, rows.Err()
}

// heads returns each stream's highest version at or below upto with its hash.
func (s *Store) heads(ctx context.Context, upto uint64) ([]es.StreamHead, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT e.stream_id, e.version, e.hash
		   FROM events e
		   JOIN (SELECT stream_id, MAX(version) AS v FROM events WHERE global_position <= ? GROUP BY stream_id) m
		     ON m.stream_id = e.stream_id AND m.v = e.version
		  ORDER BY e.stream_id`, upto)
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
		if err := rows.Scan(&canonical, &h.Version, &h.Hash); err != nil {
			return nil, err
		}
		if h.StreamID, err = es.ParseCanonical(canonical); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}
