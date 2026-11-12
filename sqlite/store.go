// Package sqlite is the SQLite backend for es-lite. It implements es.Store,
// the delivery.Checkpoints contract, and the feed log, checkpoint and lease
// ports (feed.Log/Logs, feed.CheckpointStore, feed.LeaseStore).
//
// SQLite is the first backend precisely because its single-writer model
// makes the delivery cursor gap-free for free (docs/adr/0001 §5). It is
// the file-level single-writer lock that keeps global_position gap-free
// and commit-ordered.
//
// The store also holds a single Go database connection —
// db.SetMaxOpenConns(1), enforced by Open and re-enforced by NewWithDB on
// borrowed handles. That one connection is what buys
// busy_timeout/BUSY_SNAPSHOT immunity (a read-then-write transaction
// cannot straddle two connections) and keeps a mode=memory database alive
// for the Store's lifetime. The driver must be modernc.org/sqlite:
// isConstraintViolation classifies constraint failures by matching that
// driver's error type only, so no other driver is supported.
//
// One database is one workspace. The feed ports accept a scope for parity
// with multi-tenant backends, but SQLite has no row-level security and no
// workspace column: it cannot enforce tenant isolation, and callers must not
// rely on it doing so. Named-tenant callers get a working feed over the
// single log this database holds.
package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	msqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/migrate"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Option configures Open/NewWithDB.
type Option func(*openConfig)

// defaultMaxPayloadBytes is the payload cap a Store enforces at append
// time when WithMaxPayloadBytes is not given (or given as 0): 8 MiB, big
// enough for any sane domain event and small enough that no single event
// can wedge a feed subscription that would otherwise materialize it.
const defaultMaxPayloadBytes = 8 << 20

type openConfig struct {
	autoMigrate     bool
	maxPayloadBytes int
}

// WithoutAutoMigrate skips applying migrations on Open. Application replicas
// use this and rely on a separate Migrate step (ADR 0010); the default is to
// auto-migrate (dev/SQLite/tests stay zero-config).
func WithoutAutoMigrate() Option { return func(c *openConfig) { c.autoMigrate = false } }

// WithMaxPayloadBytes caps the payload size Append may write: a batch
// holding an event whose payload exceeds n bytes is rejected whole with
// es.ErrPayloadTooLarge before anything is inserted, so writers learn at
// write time instead of wedging feed subscriptions later. n == 0 selects
// the default (8 MiB); n < 0 lifts the cap entirely.
func WithMaxPayloadBytes(n int) Option { return func(c *openConfig) { c.maxPayloadBytes = n } }

func newOpenConfig(opts []Option) openConfig {
	c := openConfig{autoMigrate: true, maxPayloadBytes: defaultMaxPayloadBytes}
	for _, o := range opts {
		o(&c)
	}
	// 0 selects the default; a negative value means unlimited. The stored
	// field is the effective cap.
	if c.maxPayloadBytes == 0 {
		c.maxPayloadBytes = defaultMaxPayloadBytes
	}
	return c
}

// tsLayout is a fixed-width RFC3339 UTC layout: 9-digit nanoseconds and a
// literal Z. Fixed width matters — it makes lexical string ordering match
// chronological ordering, so the recorded_at <= ? time-travel query works
// as a plain string comparison. (time.RFC3339Nano trims trailing zeros
// and would break that ordering.)
const tsLayout = "2006-01-02T15:04:05.000000000Z"

func formatTS(t time.Time) string         { return t.UTC().Format(tsLayout) }
func parseTS(s string) (time.Time, error) { return time.Parse(tsLayout, s) }

// Store is the SQLite-backed es.Store.
//
// Handle ownership depends on which constructor was used: Open creates the
// handle and Close closes it, whereas NewWithDB borrows a caller-provided
// handle and Close leaves it alone.
type Store struct {
	db *sql.DB

	// ownsDB records whether this Store created the *sql.DB and may
	// therefore close it. False for a handle handed in by NewWithDB,
	// where the caller may still be using it for other work.
	ownsDB bool

	// maxPayloadBytes is the effective append-time payload cap
	// (WithMaxPayloadBytes): appends holding a larger payload are rejected
	// with es.ErrPayloadTooLarge before any insert. Negative means no cap.
	maxPayloadBytes int
}

var _ es.Store = (*Store)(nil)

// Open opens (creating if needed) a SQLite database at dsn and applies
// the schema. dsn is a modernc.org/sqlite DSN, e.g.
// "file:events.db" or "file:mem?mode=memory&cache=shared". Pragmas for
// WAL, busy-timeout, and foreign keys are set automatically.
//
// The Store owns the handle it creates, so Close closes it. A caller that
// needs to manage the handle itself should use NewWithDB.
func Open(ctx context.Context, dsn string, opts ...Option) (*Store, error) {
	dsn = withPragmas(dsn)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// Single connection: serialize writers so global_position is gap-free
	// and commit-ordered, and so an in-memory shared-cache DB stays alive.
	db.SetMaxOpenConns(1)

	cfg := newOpenConfig(opts)
	s := &Store{db: db, ownsDB: true, maxPayloadBytes: cfg.maxPayloadBytes}
	if cfg.autoMigrate {
		if err := s.Migrate(ctx); err != nil {
			db.Close()
			return nil, err
		}
	}
	return s, nil
}

// NewWithDB wraps an already-open *sql.DB. The handle is borrowed, not
// owned: Close leaves it open and the caller closes it (mirroring
// postgres.OpenWithPool). NewWithDB does enforce the package's
// single-connection contract — it calls db.SetMaxOpenConns(1) whatever
// limit the handle carried — but the DSN's pragmas (WAL, busy_timeout,
// foreign_keys) remain the caller's, and the driver must be
// modernc.org/sqlite (see the package doc). Useful for tests that manage
// the handle themselves. Applies migrations unless WithoutAutoMigrate is
// passed.
func NewWithDB(ctx context.Context, db *sql.DB, opts ...Option) (*Store, error) {
	// Hold borrowed handles to the same contract as Open's (F19): more
	// than one connection forfeits the busy_timeout/BUSY_SNAPSHOT
	// immunity and memory-DB lifetime the package guarantees.
	db.SetMaxOpenConns(1)
	cfg := newOpenConfig(opts)
	s := &Store{db: db, maxPayloadBytes: cfg.maxPayloadBytes}
	if cfg.autoMigrate {
		if err := s.Migrate(ctx); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Migrate applies pending versioned migrations (ADR 0010). Each
// migration is ONE write transaction that INSERTs its schema_migrations
// row FIRST and applies the DDL second — there is no pre-check SELECT
// to race on (F34: with check-then-apply, two processes migrating one
// file DSN can both pass the check and both apply, the loser failing on
// a duplicate column or a lost version-row insert). The INSERT itself
// is the lock: SQLite allows one writer per database file, so rival
// migrators serialize on the write lock the INSERT takes, and a
// primary-key violation means the winner's row is committed — roll back
// and skip. SQLite DDL is transactional, so the version row and the
// schema change commit atomically: a recorded version always has its
// schema, and a skipped version was fully applied by the rival.
// Idempotent.
func (s *Store) Migrate(ctx context.Context) error {
	migs, err := migrate.Load(migrationsFS, "migrations")
	if err != nil {
		return err
	}
	// Bootstrap: schema_migrations must exist before the first
	// INSERT-first transaction can record anything. CREATE TABLE IF NOT
	// EXISTS is idempotent — racing bootstraps serialize on the same
	// write lock and the second is a no-op.
	if _, err := s.db.ExecContext(ctx,
		`CREATE TABLE IF NOT EXISTS schema_migrations (
			version    INTEGER PRIMARY KEY,
			applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
		)`); err != nil {
		return fmt.Errorf("sqlite: schema_migrations: %w", err)
	}
	for _, m := range migs {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		// Insert first. This statement takes the file's single write
		// lock, which serializes rival migrators; a primary-key
		// violation against a rival's committed row means this
		// migration is already applied — roll back (releasing the one
		// connection) and skip.
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES (?)`, m.Version); err != nil {
			tx.Rollback()
			if isConstraintViolation(err) {
				continue
			}
			return fmt.Errorf("sqlite: record migration %d: %w", m.Version, err)
		}
		if _, err := tx.ExecContext(ctx, m.SQL); err != nil {
			tx.Rollback()
			return fmt.Errorf("sqlite: migration %04d_%s: %w", m.Version, m.Name, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("sqlite: commit migration %d: %w", m.Version, err)
		}
	}
	return nil
}

// Close closes the underlying database when the Store owns it — when it
// came from Open. A Store built with NewWithDB borrowed the caller's
// handle and leaves it open; the caller closes it.
func (s *Store) Close() error {
	if !s.ownsDB {
		return nil
	}
	return s.db.Close()
}

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
	for _, op := range p.Constraints {
		if err := op.Validate(); err != nil {
			return zero, err
		}
	}

	// Payload cap (production review): reject the whole batch before the
	// transaction is even opened — nothing is inserted, no position drawn.
	// A single oversized event otherwise wedges every feed subscription
	// consuming its type. The error carries stream, version, type URL and
	// size only — never the payload bytes.
	for i, ev := range p.Events {
		if s.maxPayloadBytes > 0 && len(ev.Payload) > s.maxPayloadBytes {
			version := p.ExpectedVersion + uint64(i) + 1
			return zero, fmt.Errorf("%w: stream %s v%d, type %s, payload %d bytes exceeds cap %d",
				es.ErrPayloadTooLarge, p.StreamID, version, ev.TypeURL, len(ev.Payload), s.maxPayloadBytes)
		}
	}

	// Timestamps are truncated to microseconds — the precision the hash chain
	// attests and every backend returns (ADR 0029 §2).
	occurred := p.OccurredAt
	if occurred.IsZero() {
		occurred = time.Now()
	}
	occurred = es.TruncateMicros(occurred)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return zero, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	// Optimistic-concurrency guard and chain head in one read (ADR 0029 §1).
	// With a single connection this check and the inserts are effectively
	// serialized; the UNIQUE(stream_id, version) constraint is the backstop
	// if that ever changes.
	current, prev, err := readHead(ctx, tx, p.StreamID)
	if err != nil {
		return zero, err
	}
	if current != p.ExpectedVersion {
		// Structured, not formatted — same contract as the Postgres
		// backend: both versions reach the caller as fields, Error()
		// renders the text this path used to format, and errors.Is(err,
		// es.ErrConflict) still matches through ConflictError.Unwrap.
		return zero, &es.ConflictError{StreamID: p.StreamID, Expected: p.ExpectedVersion, Current: current}
	}

	// Commit clock, sampled AFTER the version check inside the transaction:
	// the loser of a version race never commits, so recorded_at is monotonic
	// per stream and an as-of-time read cannot skip a version (ADR 0029 §2,
	// review finding ES-03).
	recorded := es.TruncateMicros(time.Now())

	canonical := p.StreamID.Canonical()
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
			Version:       version,
			TypeURL:       ev.TypeURL,
			SchemaVersion: ev.SchemaVersion,
			OccurredAt:    occurred,
			RecordedAt:    recorded,
			CorrelationID: p.CorrelationID,
			CausationID:   p.CausationID,
			CommandID:     p.CommandID,
			Actor:         p.Actor,
			Payload:       ev.Payload,
			PrevHash:      append([]byte(nil), prev[:]...), // copy: prev is rewritten below
		}
		hash := es.EnvelopeHash(env)
		env.Hash = hash[:]
		prev = hash // within one batch, event i chains to event i-1 in memory

		res, err := tx.ExecContext(ctx,
			`INSERT INTO events (
				event_id, stream_type, stream_id, version,
				type_url, schema_version, occurred_at, recorded_at,
				correlation_id, causation_id, command_id,
				actor_type, actor_id, actor_principal, payload,
				prev_hash, hash
			) VALUES (?,?,?,?, ?,?,?,?, ?,?,?, ?,?,?,?, ?,?)`,
			eventID.String(), p.StreamID.Type, canonical, version,
			ev.TypeURL, ev.SchemaVersion, formatTS(occurred), formatTS(recorded),
			p.CorrelationID.String(), p.CausationID.String(), p.CommandID.String(),
			p.Actor.Type, p.Actor.ID, p.Actor.Principal(), ev.Payload,
			env.PrevHash, env.Hash,
		)
		if err != nil {
			if isConstraintViolation(err) {
				// The modernc message names the violated index definition
				// (F25): events.event_id is the duplicate-EventID case —
				// the event itself is a rerun, and retrying can never win —
				// while events.stream_id, events.version is the
				// version-race backstop and keeps today's ErrConflict. An
				// unrecognized violation falls back to the conflict arm.
				if isDuplicateEventIDViolation(err) {
					return zero, fmt.Errorf("%w: %s at v%d: %v", es.ErrDuplicateEventID, p.StreamID, version, err)
				}
				// UNIQUE(stream_id, version) names the version already
				// taken, so Current is the version that cost us the race
				// and a lower bound on the head — the Postgres backstop's
				// contract, held to for parity. An unrecognized violation
				// reaches here too (the arm is the fallback), and for it
				// Current is only the version this insert attempted; that
				// was already true of the message this replaces.
				return zero, &es.ConflictError{StreamID: p.StreamID, Expected: p.ExpectedVersion, Current: version}
			}
			return zero, fmt.Errorf("insert event v%d: %w", version, err)
		}
		gp, err := res.LastInsertId()
		if err != nil {
			return zero, fmt.Errorf("last insert id: %w", err)
		}
		env.GlobalPosition = uint64(gp)
		envs[i] = env
	}

	// Log-order attestation (design D1, F11): one log_order row per
	// non-empty batch, hashed and inserted in THIS transaction from
	// exactly the (position, hash) pairs the loop above already
	// collected — no new reads, no new locks, no cross-transaction
	// dependency, so the append path gains no serialization. The
	// entries go in loop order, which is position order: the single
	// writer assigns rowids in insert order. Anchors aggregate these
	// rows into the order root that attests the global append order,
	// and `es-verify order` recomputes each row's hash from the events
	// in its range.
	if len(p.Events) > 0 {
		entries := make([]es.OrderEntry, len(envs))
		for i := range envs {
			entries[i] = es.OrderEntry{Position: envs[i].GlobalPosition, Hash: envs[i].Hash}
		}
		batchHash := es.BatchOrderHash(entries)
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO log_order (from_position, to_position, stream_id, event_count, batch_hash)
			 VALUES (?,?,?,?,?)`,
			envs[0].GlobalPosition, envs[len(envs)-1].GlobalPosition,
			canonical, len(envs), batchHash[:],
		); err != nil {
			return zero, fmt.Errorf("insert log_order: %w", err)
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

// readHead returns the stream's current version and the prev_hash the next
// event must chain to: ZeroHash for an empty stream, the head's hash when it
// is chained, and UnchainedHeadHash when the head predates migration 0002
// (ADR 0029 §5).
func readHead(ctx context.Context, tx *sql.Tx, sid es.StreamID) (uint64, [es.HashSize]byte, error) {
	var (
		current uint64
		head    []byte
		prev    [es.HashSize]byte
	)
	err := tx.QueryRowContext(ctx,
		`SELECT version, hash FROM events WHERE stream_id = ? ORDER BY version DESC LIMIT 1`,
		sid.Canonical(),
	).Scan(&current, &head)
	switch {
	case errors.Is(err, sql.ErrNoRows):
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

// applyClaims applies uniqueness ops in the append transaction: releases
// first (so a same-value rename does not self-collide), then claims. A
// colliding claim returns es.ErrConstraintViolated, rolling back the append.
// Values are stored exactly as given; callers must pre-hash personal data
// (ADR 0025).
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
//
// A read from the beginning of a stream that was never written —
// fromVersion 0 and no rows for the stream at all, whatever toVersion is —
// returns an error wrapping es.ErrStreamNotFound (with the canonical
// stream id in the message) instead of a silent empty slice. An empty
// range is not "not found": reads with fromVersion > 0, and reads from 0
// of a stream that exists, return (nil, nil).
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
	envs, err := s.queryEnvelopes(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	if fromVersion == 0 && len(envs) == 0 {
		// A read from the beginning of the stream matched nothing. With no
		// upper bound that already proves no row exists — the stream was
		// never written. A bound makes the empty range ambiguous (rows
		// deleted out-of-band can survive only above it), so only an
		// existence probe can tell "never written" from "empty range on
		// an existing stream" — and only the first is ErrStreamNotFound.
		if toVersion > 0 {
			exists, err := s.streamExists(ctx, sid)
			if err != nil {
				return nil, err
			}
			if exists {
				return envs, nil
			}
		}
		return nil, fmt.Errorf("%w: %s", es.ErrStreamNotFound, sid.Canonical())
	}
	return envs, nil
}

// streamExists reports whether the stream has any event row at all — the
// existence probe behind ReadStream's ErrStreamNotFound: a read from the
// start that matched nothing is "not found" only when nothing was ever
// written to the stream.
func (s *Store) streamExists(ctx context.Context, sid es.StreamID) (bool, error) {
	var one int
	err := s.db.QueryRowContext(ctx,
		`SELECT 1 FROM events WHERE stream_id = ? LIMIT 1`, sid.Canonical(),
	).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
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

// LookupClaim reads the uniqueness index for the stream holding (scope, value).
// The value is stored exactly as given; callers must pass the same pre-hashed
// value both times (ADR 0025).
func (s *Store) LookupClaim(ctx context.Context, scope, value string) (string, bool, error) {
	var streamID string
	err := s.db.QueryRowContext(ctx,
		`SELECT stream_id FROM unique_claims WHERE scope = ? AND value_key = ?`,
		scope, []byte(value),
	).Scan(&streamID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return streamID, true, nil
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
	actor_type, actor_id, payload, prev_hash, hash`

func (s *Store) queryEnvelopes(ctx context.Context, q string, args ...any) ([]es.Envelope, error) {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []es.Envelope
	for rows.Next() {
		e, err := scanEnvelope(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// scanEnvelope scans one row selected with envelopeCols. It is the single
// column list/scan pairing shared by every read, including the feed page.
// The audit UUIDs are load-bearing (F34): a stored event_id,
// correlation_id, causation_id or command_id that does not parse fails the
// read rather than being silently zeroed into a forged envelope.
func scanEnvelope(rows *sql.Rows) (es.Envelope, error) {
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
		&actorType, &actorID, &e.Payload, &e.PrevHash, &e.Hash,
	); err != nil {
		return es.Envelope{}, err
	}
	var err error
	if e.EventID, err = parseStoredUUID("event_id", eventID); err != nil {
		return es.Envelope{}, err
	}
	if e.StreamID, err = es.ParseCanonical(streamID); err != nil {
		return es.Envelope{}, fmt.Errorf("parse stream_id %q: %w", streamID, err)
	}
	if e.OccurredAt, err = parseTS(occurredAt); err != nil {
		return es.Envelope{}, fmt.Errorf("parse occurred_at %q: %w", occurredAt, err)
	}
	if e.RecordedAt, err = parseTS(recordedAt); err != nil {
		return es.Envelope{}, fmt.Errorf("parse recorded_at %q: %w", recordedAt, err)
	}
	if e.CorrelationID, err = parseStoredUUID("correlation_id", correlationID); err != nil {
		return es.Envelope{}, err
	}
	if e.CausationID, err = parseStoredUUID("causation_id", causationID); err != nil {
		return es.Envelope{}, err
	}
	if e.CommandID, err = parseStoredUUID("command_id", commandID); err != nil {
		return es.Envelope{}, err
	}
	e.Actor = es.Actor{Type: actorType, ID: actorID}
	return e, nil
}

// parseStoredUUID parses one stored audit-UUID column (event_id,
// correlation_id, causation_id, command_id). A value that does not parse
// fails the whole read (F34): the old code zeroed it and handed back an
// envelope whose audit trail pointed at nothing. The error names the
// column and the value's LENGTH only — never the value's bytes, which
// must not leak into logs, and never uuid.Parse's own error, whose
// message quotes the offending string.
func parseStoredUUID(column, value string) (uuid.UUID, error) {
	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil, fmt.Errorf("stored %s is not a valid uuid: length %d", column, len(value))
	}
	return id, nil
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

// isDuplicateEventIDViolation reports whether a failed events INSERT hit
// the event_id UNIQUE constraint (F25). modernc.org/sqlite names the
// violated index definition in its message — "UNIQUE constraint failed:
// events.event_id" for this one, "UNIQUE constraint failed:
// events.stream_id, events.version" for the version backstop — so the
// column list in the message is the only discriminator.
func isDuplicateEventIDViolation(err error) bool {
	se, ok := errors.AsType[*msqlite.Error](err)
	if !ok {
		return false
	}
	return strings.Contains(se.Error(), "events.event_id")
}

func newV7() uuid.UUID {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.New()
	}
	return id
}
