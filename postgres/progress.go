package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

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

	// channel is the opt-in pg_notify channel for checkpoint hints. It is
	// a capability: never logged, never echoed into an error outside
	// construction validation. nil notifier means "off" — an empty
	// channel starts no goroutine and issues no statement.
	channel string

	notifier *progressNotifier
}

var _ projection.ProgressStore = (*ProgressStore)(nil)

// NewProgressStore returns a ProgressStore backed by s. channel is the
// pg_notify channel for checkpoint hints; "" disables them entirely. A
// non-empty channel must be a strict PostgreSQL identifier — the same
// rule as WithAppendNotify and NotifyListener — and a bad one panics at
// construction, a wiring bug rather than a save that silently never
// hints.
//
// Every committed mutation of a checkpoint (SaveProgress, ResetProgress,
// SetStatus) marks its scope. The notifier runs one goroutine; Close
// stops it. Callers that configured a channel must Close the store when
// done.
func NewProgressStore(s *Store, channel string) *ProgressStore {
	return newProgressStore(s, channel, progressNotifyInterval, storeProgressExec(s))
}

// newProgressStore is NewProgressStore with the two test seams — the
// coalescing window and the flush executor — supplied by the caller. The
// executor is fixed before the notifier goroutine starts, so a test can
// inject a recorder or a failing executor without racing a field write.
func newProgressStore(s *Store, channel string, interval time.Duration, exec progressNotifyExec) *ProgressStore {
	p := &ProgressStore{store: s, channel: channel}
	if channel == "" {
		return p
	}
	if err := validNotifyChannel("ProgressStore", channel); err != nil {
		panic(err)
	}
	p.notifier = newProgressNotifier(channel, exec, interval, progressNotifyBatchMax)
	return p
}

// storeProgressExec binds the notifier's flush to the store's pool. The
// channel and scopes are bound parameters, never interpolated SQL.
func storeProgressExec(s *Store) progressNotifyExec {
	return func(ctx context.Context, sql string, args ...any) error {
		_, err := s.Pool().Exec(ctx, sql, args...)
		return err
	}
}

// Close stops the checkpoint notifier, flushing any pending hint once,
// best-effort. A store opened with an empty channel has no goroutine, so
// Close is a no-op. It does not close the underlying Store.
func (p *ProgressStore) Close() error {
	if p.notifier == nil {
		return nil
	}
	return p.notifier.Close()
}

// NotifyDropped reports how many checkpoint hints were dropped since the
// store was created: a scope over maxNotifyWorkspaceBytes (the hint is
// skipped, never a save failure) and a failed flush (the batch is not
// retried). It is the runbook's loss metric, not an error to act on, and
// is 0 when the notifier is disabled. It never blocks.
func (p *ProgressStore) NotifyDropped() uint64 {
	if p.notifier == nil {
		return 0
	}
	return p.notifier.dropped.Load()
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
//
// When a hint channel is configured, the scope is marked only after
// runWorkspace reports a committed transaction: the hint can never
// describe a save that rolled back, and a failed hint can never fail the
// save. The payload is the scope alone; see progressNotifier. An
// oversized scope saves normally and the hint is skipped (NotifyDropped
// counts it): enabling hints never changes the write contract.
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
	// Emit only after the commit. Mark never blocks and never returns an
	// error, so the hint is invisible to the save's outcome.
	p.notifier.Mark(scope)
	return nil
}

// ResetProgress implements projection.ProgressStore: it replaces the row
// with position 0 and state, creating it when absent, and returns the
// stored checkpoint. A committed reset marks its scope — a rewind is
// progress a waiter must not miss.
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
	// The row is committed. Mark before validating the returned state: a
	// parse failure below cannot un-commit the write, so the scope must
	// still be refreshed.
	p.notifier.Mark(scope)
	parsed, ok := projection.ParseStatus(gotState)
	if !ok || parsed == projection.StatusUnknown {
		return projection.Checkpoint{}, fmt.Errorf("postgres: progress: reset: unknown state %q for (%s, %s)", gotState, scope, projectionName)
	}
	cp.Scope, cp.Projection, cp.State = scope, projectionName, parsed
	return cp, nil
}

// SetStatus implements projection.ProgressStore: it changes only the state
// and UpdatedAt, creating a zero-position row when absent, and never moves
// the position. A committed status change marks its scope: a waiter's
// cached state must not survive a stall or rebuild transition.
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
	p.notifier.Mark(scope)
	return nil
}
