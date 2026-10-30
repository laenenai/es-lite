package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/laenenai/es-lite/es"
)

// wsStore is an es.Store scoped to one workspace. Every operation runs in a
// transaction that sets app.workspace_id (RLS) and, for partition pruning,
// filters by workspace_id explicitly. Payloads are stored and returned as
// opaque bytes (ADR 0025) — es-lite performs no encryption.
type wsStore struct {
	store *Store
	ws    string
}

var _ es.Store = (*wsStore)(nil)

// setWorkspace binds the RLS variable for the current transaction.
func setWorkspace(ctx context.Context, tx pgx.Tx, ws string) error {
	// set_config(name, value, is_local=true) — parameterized, unlike SET.
	_, err := tx.Exec(ctx, `SELECT set_config('app.workspace_id', $1, true)`, ws)
	return err
}

// Append implements es.Store.
func (w *wsStore) Append(ctx context.Context, p es.AppendParams) (es.AppendResult, error) {
	var zero es.AppendResult
	if err := p.StreamID.Validate(); err != nil {
		return zero, err
	}
	for _, op := range p.Constraints {
		if err := op.Validate(); err != nil {
			return zero, err
		}
	}

	// Timestamps are truncated to microseconds — timestamptz precision and
	// what the hash chain attests (ADR 0029 §2).
	occurred := p.OccurredAt
	if occurred.IsZero() {
		occurred = time.Now()
	}
	occurred = es.TruncateMicros(occurred)
	canonical := p.StreamID.Canonical()

	tx, err := w.store.pool.Begin(ctx)
	if err != nil {
		return zero, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := setWorkspace(ctx, tx, w.ws); err != nil {
		return zero, fmt.Errorf("set workspace: %w", err)
	}

	// Optimistic-concurrency guard and chain head in one read (ADR 0029 §1).
	current, prev, err := w.readHead(ctx, tx, p.StreamID)
	if err != nil {
		return zero, err
	}
	if current != p.ExpectedVersion {
		return zero, fmt.Errorf("%w: stream %s at v%d, expected v%d",
			es.ErrConflict, p.StreamID, current, p.ExpectedVersion)
	}

	// Commit clock, sampled AFTER the version check inside the transaction
	// (clock_timestamp, not now() = transaction start): the loser of a
	// version race never commits, so recorded_at is monotonic per stream
	// and an as-of-time read cannot skip a version (ADR 0029 §2, ES-03).
	var recorded time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&recorded); err != nil {
		return zero, fmt.Errorf("read commit clock: %w", err)
	}
	recorded = es.TruncateMicros(recorded)

	envs := make([]es.Envelope, len(p.Events))
	for i, ev := range p.Events {
		version := p.ExpectedVersion + uint64(i) + 1
		eventID := ev.EventID
		if eventID == uuid.Nil {
			eventID = newV7()
		}
		env := es.Envelope{
			EventID:       eventID,
			StreamID:      p.StreamID,
			Workspace:     w.ws,
			Version:       version,
			TypeURL:       ev.TypeURL,
			SchemaVersion: ev.SchemaVersion,
			OccurredAt:    occurred,
			RecordedAt:    recorded,
			CorrelationID: p.CorrelationID,
			CausationID:   p.CausationID,
			CommandID:     p.CommandID,
			Actor:         p.Actor,
			Payload:       ev.Payload,                      // opaque bytes, echoed back unchanged
			PrevHash:      append([]byte(nil), prev[:]...), // copy: prev is rewritten below
		}
		hash := es.EnvelopeHash(env)
		env.Hash = hash[:]
		prev = hash // within one batch, event i chains to event i-1 in memory

		var gp uint64
		err := tx.QueryRow(ctx,
			`INSERT INTO events (
				workspace_id, event_id, stream_type, stream_id, version,
				type_url, schema_version, occurred_at, recorded_at,
				correlation_id, causation_id, command_id,
				actor_type, actor_id, actor_principal, payload,
				prev_hash, hash
			) VALUES ($1,$2,$3,$4,$5, $6,$7,$8,$9, $10,$11,$12, $13,$14,$15,$16, $17,$18)
			RETURNING global_position`,
			w.ws, eventID.String(), p.StreamID.Type, canonical, version,
			ev.TypeURL, ev.SchemaVersion, occurred, recorded,
			p.CorrelationID.String(), p.CausationID.String(), p.CommandID.String(),
			p.Actor.Type, p.Actor.ID, p.Actor.Principal(), ev.Payload,
			env.PrevHash, env.Hash,
		).Scan(&gp)
		if err != nil {
			if isUniqueViolation(err) {
				return zero, fmt.Errorf("%w: stream %s v%d", es.ErrConflict, p.StreamID, version)
			}
			return zero, fmt.Errorf("insert v%d: %w", version, err)
		}
		env.GlobalPosition = gp
		envs[i] = env
	}

	if err := w.applyClaims(ctx, tx, canonical, p.Constraints); err != nil {
		return zero, err
	}

	if err := tx.Commit(ctx); err != nil {
		return zero, fmt.Errorf("commit: %w", err)
	}
	return es.AppendResult{
		FromVersion: p.ExpectedVersion,
		ToVersion:   p.ExpectedVersion + uint64(len(p.Events)),
		Envelopes:   envs,
	}, nil
}

// readHead returns the stream's current version and the prev_hash the next
// event must chain to: ZeroHash for an empty stream, the head's hash when it
// is chained, and UnchainedHeadHash when the head predates migration 0002
// (ADR 0029 §5).
func (w *wsStore) readHead(ctx context.Context, tx pgx.Tx, sid es.StreamID) (uint64, [es.HashSize]byte, error) {
	var (
		current uint64
		head    []byte
		prev    [es.HashSize]byte
	)
	err := tx.QueryRow(ctx,
		`SELECT version, hash FROM events WHERE workspace_id = $1 AND stream_id = $2 ORDER BY version DESC LIMIT 1`,
		w.ws, sid.Canonical(),
	).Scan(&current, &head)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, es.ZeroHash, nil
	case err != nil:
		return 0, prev, fmt.Errorf("read stream head: %w", err)
	case len(head) == es.HashSize:
		copy(prev[:], head)
		return current, prev, nil
	default:
		return current, es.UnchainedHeadHash(sid, current), nil
	}
}

// applyClaims applies uniqueness ops in the append transaction (ADR 0008):
// releases first (so a same-value rename does not self-collide), then claims.
// The value_key is stored opaquely (ADR 0025): for personal data the caller
// supplies a keyed MAC as the value; es-lite stores whatever bytes it is given.
// A colliding claim returns es.ErrConstraintViolated, rolling back the append.
func (w *wsStore) applyClaims(ctx context.Context, tx pgx.Tx, streamID string, ops []es.ConstraintOp) error {
	for _, op := range ops {
		if op.Op != es.ReleaseOp {
			continue
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM unique_claims WHERE workspace_id=$1 AND scope=$2 AND value_key=$3 AND stream_id=$4`,
			w.ws, op.Scope, []byte(op.Value), streamID,
		); err != nil {
			return fmt.Errorf("release claim %s: %w", op.Scope, err)
		}
	}
	for _, op := range ops {
		if op.Op != es.ClaimOp {
			continue
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO unique_claims (workspace_id, scope, value_key, stream_id) VALUES ($1,$2,$3,$4)`,
			w.ws, op.Scope, []byte(op.Value), streamID,
		); err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("%w: %s=%q", es.ErrConstraintViolated, op.Scope, op.Value)
			}
			return fmt.Errorf("claim %s: %w", op.Scope, err)
		}
	}
	return nil
}

func (w *wsStore) ReadStream(ctx context.Context, sid es.StreamID, fromVersion, toVersion uint64) ([]es.Envelope, error) {
	if err := sid.Validate(); err != nil {
		return nil, err
	}
	q := `SELECT ` + envelopeCols + ` FROM events
	      WHERE workspace_id = $1 AND stream_id = $2 AND version > $3`
	args := []any{w.ws, sid.Canonical(), fromVersion}
	if toVersion > 0 {
		q += ` AND version <= $4`
		args = append(args, toVersion)
	}
	q += ` ORDER BY version ASC`
	return w.query(ctx, q, args...)
}

func (w *wsStore) ReadStreamAsOf(ctx context.Context, sid es.StreamID, asOf time.Time) ([]es.Envelope, error) {
	if err := sid.Validate(); err != nil {
		return nil, err
	}
	q := `SELECT ` + envelopeCols + ` FROM events
	      WHERE workspace_id = $1 AND stream_id = $2 AND recorded_at <= $3
	      ORDER BY version ASC`
	return w.query(ctx, q, w.ws, sid.Canonical(), asOf.UTC())
}

// ReadAll here is workspace-scoped: this workspace's events in global order
// (useful for per-workspace projections/rebuild). Cross-workspace delivery
// uses Store.Drain.
func (w *wsStore) ReadAll(ctx context.Context, fromPosition uint64, limit int) ([]es.Envelope, error) {
	if limit <= 0 {
		limit = 100
	}
	q := `SELECT ` + envelopeCols + ` FROM events
	      WHERE workspace_id = $1 AND global_position > $2
	      ORDER BY global_position ASC LIMIT $3`
	return w.query(ctx, q, w.ws, fromPosition, limit)
}

func (w *wsStore) CurrentStreamVersion(ctx context.Context, sid es.StreamID) (uint64, error) {
	if err := sid.Validate(); err != nil {
		return 0, err
	}
	tx, err := w.store.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if err := setWorkspace(ctx, tx, w.ws); err != nil {
		return 0, err
	}
	var v uint64
	err = tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM events WHERE workspace_id = $1 AND stream_id = $2`,
		w.ws, sid.Canonical(),
	).Scan(&v)
	return v, err
}

// LookupClaim reads the uniqueness index (unique_claims) for the stream holding
// (scope, value). The value is keyed exactly as applyClaims stores it — opaque
// bytes as given (ADR 0025) — so writes and lookups agree. A personal-data
// lookup must pass the same caller-computed MAC the write used.
func (w *wsStore) LookupClaim(ctx context.Context, scope, value string) (string, bool, error) {
	tx, err := w.store.pool.Begin(ctx)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback(ctx)
	if err := setWorkspace(ctx, tx, w.ws); err != nil {
		return "", false, err
	}
	var streamID string
	err = tx.QueryRow(ctx,
		`SELECT stream_id FROM unique_claims WHERE workspace_id = $1 AND scope = $2 AND value_key = $3`,
		w.ws, scope, []byte(value),
	).Scan(&streamID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return streamID, true, nil
}

// query runs a workspace-scoped read: a transaction with the RLS variable set.
// Payloads are returned opaquely (ADR 0025) — es-lite performs no decryption.
func (w *wsStore) query(ctx context.Context, q string, args ...any) ([]es.Envelope, error) {
	tx, err := w.store.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if err := setWorkspace(ctx, tx, w.ws); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	var out []es.Envelope
	for rows.Next() {
		e, err := scanEnvelope(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
