// Package postgres is the Postgres backend for es-lite: shared tables with
// per-workspace RLS and hash partitioning (ADR 0004), and a gap-safe delivery
// claim-drain (ADR 0001 §5).
//
// It is zero-knowledge about payload content (ADR 0025): payloads are stored
// and returned as opaque bytes. Any encryption is the caller's job — a
// client-side codec seals payloads before Append and opens them after read —
// so the store never holds a key.
//
// Usage:
//
//	store, _ := postgres.Open(ctx, dsn)
//	ws := store.Workspace("ws_123")                 // an es.Store scoped + RLS-bound
//	rt := aggregate.NewRuntime(ws, decider, codec)
//
// The relay reads across workspaces via store.Drain.
package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/migrate"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// migrateLockKey is the fixed advisory-lock key held while migrating, so
// concurrent migrators serialize (ADR 0010).
const migrateLockKey int64 = 0x65736C697465 // "eslite"

// Option configures Open.
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
// use this and rely on a separate Migrate step / init container (ADR 0010);
// the default auto-migrates so dev/single-node stays zero-config.
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

// Store is the Postgres-backed event store. Construct per process; it holds a
// connection pool. It is zero-knowledge — payloads are stored opaquely.
//
// Pool ownership depends on which constructor was used: Open creates the pool
// and Close releases it, whereas OpenWithPool borrows a caller-provided pool
// and Close leaves it alone.
type Store struct {
	pool *pgxpool.Pool

	// ownsPool records whether this Store created the pool and may therefore
	// close it. False for a pool handed in by OpenWithPool, where the caller
	// may still be using it for other work.
	ownsPool bool

	// maxPayloadBytes is the effective append-time payload cap
	// (WithMaxPayloadBytes): appends holding a larger payload are rejected
	// with es.ErrPayloadTooLarge before any insert. Negative means no cap.
	maxPayloadBytes int

	// relayRoleMu guards the es_relay membership verdict below — the
	// startup check every RLS-bypass path shares — Drain and the anchor
	// path's bypassTx (checkRelayRole). Migration 0005 gates the RLS
	// bypass on that membership. The verdict is cached on the Store
	// rather than the package: a process is expected to hold one Store
	// (see Store's doc), so this is once per process, and a Store opened
	// later — a different principal, or after the runbook's GRANT —
	// re-verifies instead of inheriting another pool's verdict.
	relayRoleMu sync.Mutex

	// relayRoleChecked records that the probe reached a definitive
	// verdict, held in relayRoleMember. Until it does — including after
	// a transient probe failure such as a cancelled context or a flaky
	// connection — every bypass call re-probes, so one blip cannot
	// poison the Store's lifetime (F4). Both fields are read and
	// written only under relayRoleMu.
	relayRoleChecked bool
	relayRoleMember  bool
}

// Open connects to Postgres and applies migrations (unless WithoutAutoMigrate).
// The Store owns the pool it creates, so Close releases it.
//
// Open builds the pool with pgxpool.New and default settings. A caller that
// needs control over pool construction — connection limits, application_name,
// a pgx.QueryTracer for OpenTelemetry, an AfterConnect hook — should build the
// pool itself and use OpenWithPool.
func Open(ctx context.Context, dsn string, opts ...Option) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	if err := probePostgresFloor(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	cfg := newOpenConfig(opts)
	s := &Store{pool: pool, ownsPool: true, maxPayloadBytes: cfg.maxPayloadBytes}
	if err := s.migrateIfConfigured(ctx, cfg); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// OpenWithPool wraps a pool the caller already built, instead of opening one.
//
// Use it when pool construction has to be the caller's: connection limits and
// lifetimes, application_name, a pgx.QueryTracer so every query is reported as
// an OpenTelemetry span, an AfterConnect hook registering extension types — none
// of which Open exposes. It also lets a process that already holds a pool share
// it rather than opening a second one against the same database.
//
// Ownership stays with the caller. Close does NOT close a pool passed here, so
// the caller remains free to use it for other work and is responsible for
// closing it once nothing needs it. Closing it while this Store is still in use
// is the caller's bug to avoid.
//
// Migrations behave exactly as in Open: applied unless WithoutAutoMigrate. If
// they fail, the pool is left open, since it is not this Store's to close.
func OpenWithPool(ctx context.Context, pool *pgxpool.Pool, opts ...Option) (*Store, error) {
	if pool == nil {
		return nil, fmt.Errorf("postgres: open with pool: pool is nil")
	}
	if err := probePostgresFloor(ctx, pool); err != nil {
		return nil, err
	}
	cfg := newOpenConfig(opts)
	s := &Store{pool: pool, ownsPool: false, maxPayloadBytes: cfg.maxPayloadBytes}
	if err := s.migrateIfConfigured(ctx, cfg); err != nil {
		return nil, err
	}
	return s, nil
}

// probePostgresFloor is the PostgreSQL 14 floor check both constructors run
// once, before any migration (F33): the append path's P1 xid draw and the
// horizon fence both call pg_current_xact_id(), so a server without it
// would otherwise fail every append and every fence round deep inside with
// a confusing "function pg_current_xact_id() does not exist". The 14+
// floor is the support policy, not a single feature gate:
// pg_current_xact_id() — and the pg_current_snapshot/pg_snapshot_xmin pair
// the fence reads — exist from PostgreSQL 13, so the probe mechanically
// rejects only servers older than 13 and a 13 server passes it; the
// docs state the floor as policy with the probe as the tripwire. The
// undefined-function error becomes a clear message naming the minimum;
// any other failure (a dead DSN, a cancelled context) is a probe failure,
// not a version verdict, and is wrapped as such.
func probePostgresFloor(ctx context.Context, pool *pgxpool.Pool) error {
	var xid uint64
	err := pool.QueryRow(ctx, `SELECT pg_current_xact_id()`).Scan(&xid)
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42883" { // undefined_function
		return fmt.Errorf("postgres: this server is older than PostgreSQL 14, the minimum es-lite supports (pg_current_xact_id is unavailable: %w)", err)
	}
	return fmt.Errorf("postgres: version probe: %w", err)
}

// migrateIfConfigured applies migrations unless the options disable them. It
// exists so Open and OpenWithPool cannot drift apart on migration behaviour.
func (s *Store) migrateIfConfigured(ctx context.Context, cfg openConfig) error {
	if !cfg.autoMigrate {
		return nil
	}
	return s.Migrate(ctx)
}

// Migrate applies pending versioned migrations (ADR 0010). Each pending
// migration runs in exactly one transaction — BEGIN; SELECT
// pg_advisory_xact_lock($1); re-check the recorded version; apply;
// record; COMMIT — and the schema_migrations bootstrap runs inside a
// transaction of the same shape.
//
// The lock is transaction-scoped on purpose (F10): the session-scoped
// pg_advisory_lock this path used to take stayed latched on a pooled
// connection whenever the run was cancelled or failed (the deferred
// unlock never ran on a dead context), blocking every later migration
// that landed on another connection. A transaction-level advisory lock
// works "the same as pg_advisory_lock, except that the lock is
// automatically released at the end of the current transaction and
// cannot be released explicitly" (PostgreSQL docs, System Administration
// Functions: Advisory Lock Functions), so commit, rollback, cancellation
// and connection loss all release it — there is no unlock statement to
// skip.
//
// Concurrency: two migrators may both see a version pending, but the
// second to acquire the lock re-checks the recorded version inside the
// transaction and skips what the first applied, so every migrator
// observes a consistent, monotone history. Idempotent.
func (s *Store) Migrate(ctx context.Context) error {
	migs, err := migrate.Load(migrationsFS, "migrations")
	if err != nil {
		return err
	}
	// Bootstrap the history table in its own xact-locked transaction,
	// shaped exactly like a migration's: concurrent migrators may all
	// reach this on a fresh database, and CREATE TABLE IF NOT EXISTS is
	// idempotent under the lock.
	if err := s.migrateInLock(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`CREATE TABLE IF NOT EXISTS schema_migrations (
				version    int         PRIMARY KEY,
				applied_at timestamptz NOT NULL DEFAULT now()
			)`)
		return err
	}); err != nil {
		return fmt.Errorf("postgres: schema_migrations: %w", err)
	}
	// The recorded versions, read once as a fast path: a version listed
	// here is applied, and a version missing here may since have been
	// applied by a concurrent migrator — which is why every version the
	// read marks pending is re-checked under the lock below. The read
	// can be stale only towards "pending", and a stale miss costs an
	// empty locked skip, never a double apply.
	rows, err := s.pool.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("postgres: read schema_migrations: %w", err)
	}
	defer rows.Close()
	applied := map[int]bool{}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return fmt.Errorf("postgres: read schema_migrations: %w", err)
		}
		applied[v] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("postgres: read schema_migrations: %w", err)
	}

	for _, m := range migs {
		if applied[m.Version] {
			continue
		}
		if err := s.migrateInLock(ctx, func(tx pgx.Tx) error {
			// The re-check under the lock: a concurrent migrator may
			// have applied and recorded this version since the read
			// above; the lock serializes the two, so this verdict is
			// authoritative.
			var exists bool
			if err := tx.QueryRow(ctx,
				`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, m.Version).Scan(&exists); err != nil {
				return fmt.Errorf("postgres: check migration %d: %w", m.Version, err)
			}
			if exists {
				return nil
			}
			if _, err := tx.Exec(ctx, m.SQL); err != nil {
				return fmt.Errorf("postgres: migration %04d_%s: %w", m.Version, m.Name, err)
			}
			if _, err := tx.Exec(ctx,
				`INSERT INTO schema_migrations (version) VALUES ($1)`, m.Version); err != nil {
				return fmt.Errorf("postgres: record migration %d: %w", m.Version, err)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// migrateInLock runs fn inside one transaction that holds the migration
// advisory lock for its whole duration: BEGIN; SELECT
// pg_advisory_xact_lock($1); fn; COMMIT. The lock is transaction-level,
// so it is "automatically released at the end of the current
// transaction" (PostgreSQL docs, System Administration Functions:
// Advisory Lock Functions) — commit and rollback both end it, and a
// cancelled run cannot leave it latched on a pooled connection (F10).
//
// The rollback runs on a context detached from the caller's on purpose:
// a ctx cancelled mid-migration must still send its ROLLBACK, so the
// transaction — and with it the lock — ends at a definite point instead
// of whenever the server notices a dying connection, and the pooled
// connection survives the cancellation.
func (s *Store) migrateInLock(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin: %w", err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrateLockKey); err != nil {
		return fmt.Errorf("postgres: advisory lock: %w", err)
	}
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: commit: %w", err)
	}
	return nil
}

// Close releases the pool, but only if this Store created it. A pool supplied
// through OpenWithPool belongs to the caller and is left untouched, so Close
// stays safe to call either way.
func (s *Store) Close() {
	if s.ownsPool {
		s.pool.Close()
	}
}

// Pool exposes the underlying pool (for read-model projections colocated in
// the same database).
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Workspace returns an es.Store scoped to one workspace: every operation runs
// in a transaction that sets the RLS variable. Payloads are stored and
// returned as opaque bytes (ADR 0025) — crypto is the caller's codec.
//
// Use WorkspaceStore instead when the operation must join a transaction the
// caller owns; the returned *Workspace also exposes AppendTx/ReadStreamTx.
func (s *Store) Workspace(workspaceID string) es.Store {
	return s.WorkspaceStore(workspaceID)
}

// WorkspaceStore returns the workspace-scoped handle as its concrete type.
// It implements es.Store and additionally exposes AppendTx and ReadStreamTx,
// which run inside a caller-supplied transaction (a tenancy.TenantTx, for
// example) without opening, committing or rolling back one of their own.
func (s *Store) WorkspaceStore(workspaceID string) *Workspace {
	return &Workspace{store: s, ws: workspaceID}
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505" // unique_violation
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

const envelopeCols = `
	global_position, event_id, stream_type, stream_id, version,
	type_url, schema_version, occurred_at, recorded_at,
	correlation_id, causation_id, command_id,
	actor_type, actor_id, payload, workspace_id, prev_hash, hash`

// parseStoredUUID parses one stored audit-UUID column (event_id,
// correlation_id, causation_id, command_id). A value that does not parse
// fails the whole read (F34): the old code zeroed it and handed back an
// envelope whose audit trail pointed at nothing. The error names the column
// and the value's LENGTH only — never the value's bytes, which must not
// leak into logs, and never uuid.Parse's own error, whose message quotes
// the offending string.
func parseStoredUUID(column, value string) (uuid.UUID, error) {
	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil, fmt.Errorf("stored %s is not a valid uuid: length %d", column, len(value))
	}
	return id, nil
}

// scanEnvelope reads one row (in envelopeCols order). The payload is opaque —
// stored and returned as-is (ADR 0025); es-lite performs no decryption.
func scanEnvelope(row pgx.Row) (es.Envelope, error) {
	var (
		e                                     es.Envelope
		eventID, streamType, streamID         string
		correlationID, causationID, commandID string
		actorType, actorID                    string
		payload                               []byte
		workspaceID                           string
	)
	if err := row.Scan(
		&e.GlobalPosition, &eventID, &streamType, &streamID, &e.Version,
		&e.TypeURL, &e.SchemaVersion, &e.OccurredAt, &e.RecordedAt,
		&correlationID, &causationID, &commandID,
		&actorType, &actorID, &payload, &workspaceID, &e.PrevHash, &e.Hash,
	); err != nil {
		return es.Envelope{}, err
	}
	// pgx returns timestamptz in the session zone; the chain hashes UTC µs.
	e.OccurredAt = es.TruncateMicros(e.OccurredAt)
	e.RecordedAt = es.TruncateMicros(e.RecordedAt)
	sid, err := es.ParseCanonical(streamID)
	if err != nil {
		return es.Envelope{}, fmt.Errorf("parse stream_id %q: %w", streamID, err)
	}
	e.StreamID = sid
	e.Workspace = workspaceID
	// The audit UUIDs are load-bearing (F34): a value that does not parse
	// fails the read rather than being silently zeroed into a forged
	// envelope.
	if e.EventID, err = parseStoredUUID("event_id", eventID); err != nil {
		return es.Envelope{}, err
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
	e.Payload = payload
	return e, nil
}
