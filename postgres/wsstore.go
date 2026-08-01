package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/laenenai/es-lite/es"
)

// wsStore is an es.Store scoped to one workspace. Every operation runs in a
// transaction that sets app.workspace_id (RLS) and, for partition pruning,
// filters by workspace_id explicitly. Payloads are encrypted on write and
// decrypted on read via the workspace DEK.
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
	if len(p.Events) == 0 {
		return es.AppendResult{FromVersion: p.ExpectedVersion, ToVersion: p.ExpectedVersion}, nil
	}

	cipher, err := w.store.writeCipher(ctx, w.ws)
	if err != nil {
		return zero, err
	}

	occurred := p.OccurredAt
	if occurred.IsZero() {
		occurred = time.Now().UTC()
	}
	canonical := p.StreamID.Canonical()

	tx, err := w.store.pool.Begin(ctx)
	if err != nil {
		return zero, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := setWorkspace(ctx, tx, w.ws); err != nil {
		return zero, fmt.Errorf("set workspace: %w", err)
	}

	var current uint64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM events WHERE workspace_id = $1 AND stream_id = $2`,
		w.ws, canonical,
	).Scan(&current); err != nil {
		return zero, fmt.Errorf("read current version: %w", err)
	}
	if current != p.ExpectedVersion {
		return zero, fmt.Errorf("%w: stream %s at v%d, expected v%d",
			es.ErrConflict, p.StreamID, current, p.ExpectedVersion)
	}

	envs := make([]es.Envelope, len(p.Events))
	for i, ev := range p.Events {
		version := p.ExpectedVersion + uint64(i) + 1
		eventID := ev.EventID
		if eventID == uuid.Nil {
			eventID = newV7()
		}
		ciphertext, err := w.store.encrypt(cipher, ev.Payload)
		if err != nil {
			return zero, fmt.Errorf("encrypt v%d: %w", version, err)
		}

		var gp uint64
		var recorded time.Time
		err = tx.QueryRow(ctx,
			`INSERT INTO events (
				workspace_id, event_id, stream_type, stream_id, version,
				type_url, schema_version, occurred_at,
				correlation_id, causation_id, command_id,
				actor_type, actor_id, actor_principal, payload
			) VALUES ($1,$2,$3,$4,$5, $6,$7,$8, $9,$10,$11, $12,$13,$14,$15)
			RETURNING global_position, recorded_at`,
			w.ws, eventID.String(), p.StreamID.Type, canonical, version,
			ev.TypeURL, ev.SchemaVersion, occurred,
			p.CorrelationID.String(), p.CausationID.String(), p.CommandID.String(),
			p.Actor.Type, p.Actor.ID, p.Actor.Principal(), ciphertext,
		).Scan(&gp, &recorded)
		if err != nil {
			if isUniqueViolation(err) {
				return zero, fmt.Errorf("%w: stream %s v%d", es.ErrConflict, p.StreamID, version)
			}
			return zero, fmt.Errorf("insert v%d: %w", version, err)
		}

		envs[i] = es.Envelope{
			EventID:        eventID,
			StreamID:       p.StreamID,
			Version:        version,
			GlobalPosition: gp,
			TypeURL:        ev.TypeURL,
			SchemaVersion:  ev.SchemaVersion,
			OccurredAt:     occurred,
			RecordedAt:     recorded,
			CorrelationID:  p.CorrelationID,
			CausationID:    p.CausationID,
			CommandID:      p.CommandID,
			Actor:          p.Actor,
			Payload:        ev.Payload, // return plaintext to the caller
		}
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

// query runs a workspace-scoped read: a transaction with the RLS variable
// set. Rows are scanned encrypted, then decrypted after the result set is
// materialized — so an empty stream never needs a cipher, and a decrypt of a
// shredded workspace surfaces keystore.ErrShredded.
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
	var (
		out   []es.Envelope
		blobs [][]byte
	)
	for rows.Next() {
		e, ciphertext, err := scanEnvelope(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, e)
		blobs = append(blobs, ciphertext)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, nil
	}

	cipher, err := w.store.readCipher(ctx, w.ws)
	if err != nil {
		return nil, err // keystore.ErrShredded when the workspace is forgotten
	}
	for i := range out {
		pt, err := w.store.decrypt(cipher, blobs[i])
		if err != nil {
			return nil, err
		}
		out[i].Payload = pt
	}
	return out, nil
}
