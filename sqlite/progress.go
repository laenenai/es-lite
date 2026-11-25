package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/laenenai/es-lite/projection"
)

// ProgressStore persists projection checkpoints in SQLite (ADR 0045,
// read-your-writes). One SQLite database is one workspace, so unlike the
// Postgres adapter there is no RLS and no workspace column: the scope is
// the feed ports' parity argument (usually the empty string) and the
// database file's own boundary is the tenant boundary.
//
// The semantics mirror projection.MemoryProgressStore: a save advances the
// position monotonically within a state and replaces it on a state change
// (a rebuild restarts the position); SetStatus never moves the position.
// Status is stored as its text form and validated on both write and read,
// so StatusUnknown can never become a row. updated_at is written in the
// store's tsLayout, the same fixed-width layout as events.recorded_at.
type ProgressStore struct {
	store *Store
}

var _ projection.ProgressStore = (*ProgressStore)(nil)

// NewProgressStore returns a ProgressStore backed by s.
func NewProgressStore(s *Store) *ProgressStore {
	return &ProgressStore{store: s}
}

// progressStateText maps a persisted status to the table's text form,
// refusing StatusUnknown and out-of-range values. StatusUnknown means "no
// row", so it is never written.
func progressStateText(state projection.Status) (string, error) {
	switch state {
	case projection.StatusLive, projection.StatusRebuilding, projection.StatusStalled:
		return state.String(), nil
	default:
		return "", fmt.Errorf("sqlite: progress: invalid state %s", state)
	}
}

// LoadProgress implements projection.ProgressStore. An absent row returns
// found=false and a zero Checkpoint, no error.
func (p *ProgressStore) LoadProgress(ctx context.Context, scope, projectionName string) (projection.Checkpoint, bool, error) {
	var (
		cp        projection.Checkpoint
		position  int64
		state     string
		updatedAt string
	)
	err := p.store.db.QueryRowContext(ctx,
		`SELECT position, state, updated_at FROM projection_checkpoints
		 WHERE scope = ? AND projection = ?`,
		scope, projectionName,
	).Scan(&position, &state, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return projection.Checkpoint{}, false, nil
	}
	if err != nil {
		return projection.Checkpoint{}, false, fmt.Errorf("sqlite: progress: load: %w", err)
	}
	parsed, ok := projection.ParseStatus(state)
	if !ok || parsed == projection.StatusUnknown {
		return projection.Checkpoint{}, false, fmt.Errorf("sqlite: progress: load: unknown state %q for (%s, %s)", state, scope, projectionName)
	}
	if cp.UpdatedAt, err = parseTS(updatedAt); err != nil {
		return projection.Checkpoint{}, false, fmt.Errorf("sqlite: progress: load: parse updated_at %q: %w", updatedAt, err)
	}
	// SQLite integers are signed, so the int64→uint64 narrowing wraps above
	// 2^63 — unreachable for real positions; feed.go and snapshot.go make
	// the same cast.
	cp.Position = uint64(position)
	cp.Scope, cp.Projection, cp.State = scope, projectionName, parsed
	return cp, true, nil
}

// SaveProgress implements projection.ProgressStore. The args override
// cp.Scope and cp.Projection. A same-state save keeps the greater position;
// a different state replaces it. UpdatedAt refreshes on every save, even
// when the position does not move.
func (p *ProgressStore) SaveProgress(ctx context.Context, scope, projectionName string, cp projection.Checkpoint) error {
	state, err := progressStateText(cp.State)
	if err != nil {
		return err
	}
	_, err = p.store.db.ExecContext(ctx,
		`INSERT INTO projection_checkpoints (scope, projection, position, state, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(scope, projection) DO UPDATE SET
		     position = CASE
		         WHEN projection_checkpoints.state = excluded.state
		         THEN MAX(projection_checkpoints.position, excluded.position)
		         ELSE excluded.position END,
		     state = excluded.state,
		     updated_at = excluded.updated_at`,
		scope, projectionName, int64(cp.Position), state, formatTS(time.Now()),
	)
	if err != nil {
		return fmt.Errorf("sqlite: progress: save: %w", err)
	}
	return nil
}

// ResetProgress implements projection.ProgressStore: it replaces the row
// with position 0 and state, creating it when absent, and returns the
// stored checkpoint.
func (p *ProgressStore) ResetProgress(ctx context.Context, scope, projectionName string, state projection.Status) (projection.Checkpoint, error) {
	text, err := progressStateText(state)
	if err != nil {
		return projection.Checkpoint{}, err
	}
	var (
		cp        projection.Checkpoint
		position  int64
		gotState  string
		updatedAt string
	)
	err = p.store.db.QueryRowContext(ctx,
		`INSERT INTO projection_checkpoints (scope, projection, position, state, updated_at)
		 VALUES (?, ?, 0, ?, ?)
		 ON CONFLICT(scope, projection) DO UPDATE SET
		     position = 0,
		     state = excluded.state,
		     updated_at = excluded.updated_at
		 RETURNING position, state, updated_at`,
		scope, projectionName, text, formatTS(time.Now()),
	).Scan(&position, &gotState, &updatedAt)
	if err != nil {
		return projection.Checkpoint{}, fmt.Errorf("sqlite: progress: reset: %w", err)
	}
	parsed, ok := projection.ParseStatus(gotState)
	if !ok || parsed == projection.StatusUnknown {
		return projection.Checkpoint{}, fmt.Errorf("sqlite: progress: reset: unknown state %q for (%s, %s)", gotState, scope, projectionName)
	}
	if cp.UpdatedAt, err = parseTS(updatedAt); err != nil {
		return projection.Checkpoint{}, fmt.Errorf("sqlite: progress: reset: parse updated_at %q: %w", updatedAt, err)
	}
	cp.Position = uint64(position)
	cp.Scope, cp.Projection, cp.State = scope, projectionName, parsed
	return cp, nil
}

// SetStatus implements projection.ProgressStore: it changes only the state
// and UpdatedAt, creating a zero-position row when absent, and never moves
// the position.
func (p *ProgressStore) SetStatus(ctx context.Context, scope, projectionName string, state projection.Status) error {
	text, err := progressStateText(state)
	if err != nil {
		return err
	}
	_, err = p.store.db.ExecContext(ctx,
		`INSERT INTO projection_checkpoints (scope, projection, position, state, updated_at)
		 VALUES (?, ?, 0, ?, ?)
		 ON CONFLICT(scope, projection) DO UPDATE SET
		     state = excluded.state,
		     updated_at = excluded.updated_at`,
		scope, projectionName, text, formatTS(time.Now()),
	)
	if err != nil {
		return fmt.Errorf("sqlite: progress: set status: %w", err)
	}
	return nil
}
