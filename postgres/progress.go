package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/laenenai/es-lite/projection"
)

// ProgressStore persists projection checkpoints in Postgres (ADR 0040,
// read-your-writes). Every operation runs in one short transaction with
// app.workspace_id bound to the scope (runWorkspace), so the table's policy
// applies, and updated_at is the database's clock (clock_timestamp() in
// SQL), never the caller's.
//
// The semantics mirror projection.MemoryProgressStore: a save advances the
// position monotonically within a state and replaces it on a state change
// (a rebuild restarts the position); SetStatus never moves the position.
// Status is stored as its text form and validated on both write and read,
// so StatusUnknown can never become a row.
type ProgressStore struct {
	store *Store

	// channel is the opt-in pg_notify channel for checkpoint hints. Task 7
	// wires the notifier; until then (and when empty) no hint is emitted.
	channel string
}

var _ projection.ProgressStore = (*ProgressStore)(nil)

// NewProgressStore returns a ProgressStore backed by s. channel is the
// pg_notify channel for checkpoint hints; "" disables them (Task 7).
func NewProgressStore(s *Store, channel string) *ProgressStore {
	return &ProgressStore{store: s, channel: channel}
}

// progressStateText maps a persisted status to the table's text form,
// refusing StatusUnknown and out-of-range values. StatusUnknown means "no
// row", so it is never written.
func progressStateText(state projection.Status) (string, error) {
	switch state {
	case projection.StatusLive, projection.StatusRebuilding, projection.StatusStalled:
		return state.String(), nil
	default:
		return "", fmt.Errorf("postgres: progress: invalid state %s", state)
	}
}

// LoadProgress implements projection.ProgressStore. An absent row returns
// found=false and a zero Checkpoint, no error.
func (p *ProgressStore) LoadProgress(ctx context.Context, scope, projectionName string) (projection.Checkpoint, bool, error) {
	var (
		cp    projection.Checkpoint
		state string
	)
	err := p.store.runWorkspace(ctx, scope, func(tx DBTX) error {
		return tx.QueryRow(ctx,
			`SELECT position, state, updated_at FROM projection_checkpoints
			 WHERE workspace_id = $1 AND projection = $2`,
			scope, projectionName,
		).Scan(&cp.Position, &state, &cp.UpdatedAt)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return projection.Checkpoint{}, false, nil
	}
	if err != nil {
		return projection.Checkpoint{}, false, fmt.Errorf("postgres: progress: load: %w", err)
	}
	parsed, ok := projection.ParseStatus(state)
	if !ok || parsed == projection.StatusUnknown {
		return projection.Checkpoint{}, false, fmt.Errorf("postgres: progress: load: unknown state %q for (%s, %s)", state, scope, projectionName)
	}
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
	err = p.store.runWorkspace(ctx, scope, func(tx DBTX) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO projection_checkpoints (workspace_id, projection, position, state)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT (workspace_id, projection) DO UPDATE SET
			     position = CASE
			         WHEN projection_checkpoints.state = excluded.state
			         THEN GREATEST(projection_checkpoints.position, excluded.position)
			         ELSE excluded.position END,
			     state = excluded.state,
			     updated_at = clock_timestamp()`,
			scope, projectionName, int64(cp.Position), state,
		)
		return err
	})
	if err != nil {
		return fmt.Errorf("postgres: progress: save: %w", err)
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
		cp       projection.Checkpoint
		gotState string
	)
	err = p.store.runWorkspace(ctx, scope, func(tx DBTX) error {
		return tx.QueryRow(ctx,
			`INSERT INTO projection_checkpoints (workspace_id, projection, position, state)
			 VALUES ($1, $2, 0, $3)
			 ON CONFLICT (workspace_id, projection) DO UPDATE SET
			     position = 0,
			     state = excluded.state,
			     updated_at = clock_timestamp()
			 RETURNING position, state, updated_at`,
			scope, projectionName, text,
		).Scan(&cp.Position, &gotState, &cp.UpdatedAt)
	})
	if err != nil {
		return projection.Checkpoint{}, fmt.Errorf("postgres: progress: reset: %w", err)
	}
	parsed, ok := projection.ParseStatus(gotState)
	if !ok || parsed == projection.StatusUnknown {
		return projection.Checkpoint{}, fmt.Errorf("postgres: progress: reset: unknown state %q for (%s, %s)", gotState, scope, projectionName)
	}
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
	err = p.store.runWorkspace(ctx, scope, func(tx DBTX) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO projection_checkpoints (workspace_id, projection, position, state)
			 VALUES ($1, $2, 0, $3)
			 ON CONFLICT (workspace_id, projection) DO UPDATE SET
			     state = excluded.state,
			     updated_at = clock_timestamp()`,
			scope, projectionName, text,
		)
		return err
	})
	if err != nil {
		return fmt.Errorf("postgres: progress: set status: %w", err)
	}
	return nil
}
