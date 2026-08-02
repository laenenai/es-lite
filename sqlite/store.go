// Package sqlite is the SQLite backend for es-lite. It implements
// es.Store plus the delivery.Checkpoints contract.
//
// SQLite is the first backend precisely because its single-writer model
// makes the delivery cursor gap-free for free (docs/adr/0001 §5). The
// store keeps a single database connection so writes are serialized and
// global_position is assigned in commit order.
package sqlite

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	msqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/laenenai/es-lite/es"
)

//go:embed schema.sql
var schemaSQL string

// tsLayout is a fixed-width RFC3339 UTC layout: 9-digit nanoseconds and a
// literal Z. Fixed width matters — it makes lexical string ordering match
// chronological ordering, so the recorded_at <= ? time-travel query works
// as a plain string comparison. (time.RFC3339Nano trims trailing zeros
// and would break that ordering.)
const tsLayout = "2006-01-02T15:04:05.000000000Z"

func formatTS(t time.Time) string   { return t.UTC().Format(tsLayout) }
func parseTS(s string) (time.Time, error) { return time.Parse(tsLayout, s) }

// Store is the SQLite-backed es.Store.
type Store struct {
	db *sql.DB
}

var _ es.Store = (*Store)(nil)

// Open opens (creating if needed) a SQLite database at dsn and applies
// the schema. dsn is a modernc.org/sqlite DSN, e.g.
// "file:events.db" or "file:mem?mode=memory&cache=shared". Pragmas for
// WAL, busy-timeout, and foreign keys are set automatically.
func Open(ctx context.Context, dsn string) (*Store, error) {
	dsn = withPragmas(dsn)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// Single connection: serialize writers so global_position is gap-free
	// and commit-ordered, and so an in-memory shared-cache DB stays alive.
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &Store{db: db}, nil
}

// NewWithDB wraps an already-open *sql.DB and applies the schema. Useful
// for tests that manage the handle themselves.
func NewWithDB(ctx context.Context, db *sql.DB) (*Store, error) {
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &Store{db: db}, nil
}

// Close closes the underlying database.
func (s *Store) Close() error { return s.db.Close() }

// DB exposes the underlying handle (for read-model projections that live
// in the same database as the log).
func (s *Store) DB() *sql.DB { return s.db }

func withPragmas(dsn string) string {
	pragmas := "_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(on)"
	if strings.Contains(dsn, "?") {
		return dsn + "&" + pragmas
	}
	return dsn + "?" + pragmas
}

// Append implements es.Store.
func (s *Store) Append(ctx context.Context, p es.AppendParams) (es.AppendResult, error) {
	var zero es.AppendResult
	if err := p.StreamID.Validate(); err != nil {
		return zero, err
	}
	if len(p.Events) == 0 {
		return es.AppendResult{FromVersion: p.ExpectedVersion, ToVersion: p.ExpectedVersion}, nil
	}

	occurred := p.OccurredAt
	if occurred.IsZero() {
		occurred = time.Now().UTC()
	}
	recorded := time.Now().UTC()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return zero, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	// Optimistic-concurrency guard. With a single connection this check
	// and the inserts are effectively serialized; the UNIQUE(stream_id,
	// version) constraint is the backstop if that ever changes.
	var current uint64
	err = tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = ?`,
		p.StreamID.Canonical(),
	).Scan(&current)
	if err != nil {
		return zero, fmt.Errorf("read current version: %w", err)
	}
	if current != p.ExpectedVersion {
		return zero, fmt.Errorf("%w: stream %s at v%d, expected v%d",
			es.ErrConflict, p.StreamID, current, p.ExpectedVersion)
	}

	canonical := p.StreamID.Canonical()
	envs := make([]es.Envelope, len(p.Events))
	for i, ev := range p.Events {
		version := p.ExpectedVersion + uint64(i) + 1
		eventID := ev.EventID
		if eventID == uuid.Nil {
			eventID = newV7()
		}
		res, err := tx.ExecContext(ctx,
			`INSERT INTO events (
				event_id, stream_type, stream_id, version,
				type_url, schema_version, occurred_at, recorded_at,
				correlation_id, causation_id, command_id,
				actor_type, actor_id, actor_principal, payload
			) VALUES (?,?,?,?, ?,?,?,?, ?,?,?, ?,?,?,?)`,
			eventID.String(), p.StreamID.Type, canonical, version,
			ev.TypeURL, ev.SchemaVersion, formatTS(occurred), formatTS(recorded),
			p.CorrelationID.String(), p.CausationID.String(), p.CommandID.String(),
			p.Actor.Type, p.Actor.ID, p.Actor.Principal(), ev.Payload,
		)
		if err != nil {
			if isConstraintViolation(err) {
				// A concurrent writer claimed this version first.
				return zero, fmt.Errorf("%w: stream %s v%d", es.ErrConflict, p.StreamID, version)
			}
			return zero, fmt.Errorf("insert event v%d: %w", version, err)
		}
		gp, err := res.LastInsertId()
		if err != nil {
			return zero, fmt.Errorf("last insert id: %w", err)
		}
		envs[i] = es.Envelope{
			EventID:        eventID,
			StreamID:       p.StreamID,
			Version:        version,
			GlobalPosition: uint64(gp),
			TypeURL:        ev.TypeURL,
			SchemaVersion:  ev.SchemaVersion,
			OccurredAt:     occurred,
			RecordedAt:     recorded,
			CorrelationID:  p.CorrelationID,
			CausationID:    p.CausationID,
			CommandID:      p.CommandID,
			Actor:          p.Actor,
			Payload:        ev.Payload,
		}
	}

	if err := applyClaims(ctx, tx, canonical, p.Constraints); err != nil {
		return zero, err
	}

	if err := tx.Commit(); err != nil {
		return zero, fmt.Errorf("commit: %w", err)
	}
	return es.AppendResult{
		FromVersion: p.ExpectedVersion,
		ToVersion:   p.ExpectedVersion + uint64(len(p.Events)),
		Envelopes:   envs,
	}, nil
}

// applyClaims applies uniqueness ops in the append transaction: releases
// first (so a same-value rename does not self-collide), then claims. A
// colliding claim returns es.ErrConstraintViolated, rolling back the append.
// Values are stored plaintext (SQLite has no keystore; PII is a Postgres
// concern).
func applyClaims(ctx context.Context, tx *sql.Tx, streamID string, ops []es.ConstraintOp) error {
	for _, op := range ops {
		if op.Op != es.ReleaseOp {
			continue
		}
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM unique_claims WHERE scope = ? AND value_key = ? AND stream_id = ?`,
			op.Scope, []byte(op.Value), streamID,
		); err != nil {
			return fmt.Errorf("release claim %s=%q: %w", op.Scope, op.Value, err)
		}
	}
	for _, op := range ops {
		if op.Op != es.ClaimOp {
			continue
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO unique_claims (scope, value_key, stream_id) VALUES (?, ?, ?)`,
			op.Scope, []byte(op.Value), streamID,
		); err != nil {
			if isConstraintViolation(err) {
				return fmt.Errorf("%w: %s=%q", es.ErrConstraintViolated, op.Scope, op.Value)
			}
			return fmt.Errorf("claim %s=%q: %w", op.Scope, op.Value, err)
		}
	}
	return nil
}

// ReadStream implements es.Store.
func (s *Store) ReadStream(ctx context.Context, sid es.StreamID, fromVersion, toVersion uint64) ([]es.Envelope, error) {
	if err := sid.Validate(); err != nil {
		return nil, err
	}
	q := `SELECT ` + envelopeCols + ` FROM events WHERE stream_id = ? AND version > ?`
	args := []any{sid.Canonical(), fromVersion}
	if toVersion > 0 {
		q += ` AND version <= ?`
		args = append(args, toVersion)
	}
	q += ` ORDER BY version ASC`
	return s.queryEnvelopes(ctx, q, args...)
}

// ReadStreamAsOf implements es.Store.
func (s *Store) ReadStreamAsOf(ctx context.Context, sid es.StreamID, asOf time.Time) ([]es.Envelope, error) {
	if err := sid.Validate(); err != nil {
		return nil, err
	}
	q := `SELECT ` + envelopeCols + ` FROM events
	      WHERE stream_id = ? AND recorded_at <= ?
	      ORDER BY version ASC`
	return s.queryEnvelopes(ctx, q, sid.Canonical(), formatTS(asOf))
}

// ReadAll implements es.Store.
func (s *Store) ReadAll(ctx context.Context, fromPosition uint64, limit int) ([]es.Envelope, error) {
	if limit <= 0 {
		limit = 100
	}
	q := `SELECT ` + envelopeCols + ` FROM events
	      WHERE global_position > ?
	      ORDER BY global_position ASC
	      LIMIT ?`
	return s.queryEnvelopes(ctx, q, fromPosition, limit)
}

// CurrentStreamVersion implements es.Store.
func (s *Store) CurrentStreamVersion(ctx context.Context, sid es.StreamID) (uint64, error) {
	if err := sid.Validate(); err != nil {
		return 0, err
	}
	var v uint64
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = ?`,
		sid.Canonical(),
	).Scan(&v)
	if err != nil {
		return 0, err
	}
	return v, nil
}

const envelopeCols = `
	global_position, event_id, stream_type, stream_id, version,
	type_url, schema_version, occurred_at, recorded_at,
	correlation_id, causation_id, command_id,
	actor_type, actor_id, payload`

func (s *Store) queryEnvelopes(ctx context.Context, q string, args ...any) ([]es.Envelope, error) {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []es.Envelope
	for rows.Next() {
		var (
			e                                     es.Envelope
			eventID, streamType, streamID         string
			occurredAt, recordedAt                string
			correlationID, causationID, commandID string
			actorType, actorID                    string
		)
		if err := rows.Scan(
			&e.GlobalPosition, &eventID, &streamType, &streamID, &e.Version,
			&e.TypeURL, &e.SchemaVersion, &occurredAt, &recordedAt,
			&correlationID, &causationID, &commandID,
			&actorType, &actorID, &e.Payload,
		); err != nil {
			return nil, err
		}
		if e.EventID, err = uuid.Parse(eventID); err != nil {
			return nil, fmt.Errorf("parse event_id %q: %w", eventID, err)
		}
		if e.StreamID, err = es.ParseCanonical(streamID); err != nil {
			return nil, fmt.Errorf("parse stream_id %q: %w", streamID, err)
		}
		if e.OccurredAt, err = parseTS(occurredAt); err != nil {
			return nil, fmt.Errorf("parse occurred_at %q: %w", occurredAt, err)
		}
		if e.RecordedAt, err = parseTS(recordedAt); err != nil {
			return nil, fmt.Errorf("parse recorded_at %q: %w", recordedAt, err)
		}
		e.CorrelationID, _ = uuid.Parse(correlationID)
		e.CausationID, _ = uuid.Parse(causationID)
		e.CommandID, _ = uuid.Parse(commandID)
		e.Actor = es.Actor{Type: actorType, ID: actorID}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// LoadCheckpoint returns the last processed global_position for a
// subscriber, or 0 if it has none yet. Implements delivery.Checkpoints.
func (s *Store) LoadCheckpoint(ctx context.Context, subscriber string) (uint64, error) {
	var pos uint64
	err := s.db.QueryRowContext(ctx,
		`SELECT position FROM checkpoints WHERE subscriber = ?`, subscriber,
	).Scan(&pos)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return pos, nil
}

// SaveCheckpoint durably advances a subscriber's checkpoint. Implements
// delivery.Checkpoints.
func (s *Store) SaveCheckpoint(ctx context.Context, subscriber string, position uint64) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO checkpoints (subscriber, position, updated_at)
		 VALUES (?, ?, ?)
		 ON CONFLICT(subscriber) DO UPDATE SET position = excluded.position, updated_at = excluded.updated_at`,
		subscriber, position, formatTS(time.Now()),
	)
	return err
}

func isConstraintViolation(err error) bool {
	if se, ok := errors.AsType[*msqlite.Error](err); ok {
		code := se.Code()
		return code == sqlite3.SQLITE_CONSTRAINT ||
			code == sqlite3.SQLITE_CONSTRAINT_UNIQUE ||
			code == sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY
	}
	return false
}

func newV7() uuid.UUID {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.New()
	}
	return id
}
