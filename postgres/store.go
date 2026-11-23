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
	"strings"
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

const (
	// defaultLedgerSchema and defaultLedgerModule are the shared-ledger
	// defaults. The table is migrations.goose_migrations, outside every
	// module schema, so a migration that drops its own schema cannot erase
	// the history. It is per database: a store pointed at another DSN gets
	// that database's ledger under the same module name.
	defaultLedgerSchema = "migrations"
	defaultLedgerModule = "eventstore"
)

type openConfig struct {
	autoMigrate         bool
	maxPayloadBytes     int
	notifyChannel       string
	markerNotifyChannel string
	module              string
	ledgerSchema        string
}

// WithoutAutoMigrate skips applying migrations on Open. Application replicas
// use this and rely on a separate Migrate step / init container (ADR 0010);
// the default auto-migrates so dev/single-node stays zero-config.
func WithoutAutoMigrate() Option { return func(c *openConfig) { c.autoMigrate = false } }

// WithModule names this store's rows in the shared migration ledger. The
// default is "eventstore". The name is the stable key when a module moves to
// another database: the ledger moves with the database, the name does not.
func WithModule(name string) Option { return func(c *openConfig) { c.module = name } }

// WithLedgerSchema names the schema holding the shared ledger table. The
// default is "migrations". The schema is created only when it is absent, so a
// maintenance role can pre-create it and grant DML only.
func WithLedgerSchema(schema string) Option {
	return func(c *openConfig) { c.ledgerSchema = schema }
}

// WithMaxPayloadBytes caps the payload size Append may write: a batch
// holding an event whose payload exceeds n bytes is rejected whole with
// es.ErrPayloadTooLarge before anything is inserted, so writers learn at
// write time instead of wedging feed subscriptions later. n == 0 selects
// the default (8 MiB); n < 0 lifts the cap entirely.
func WithMaxPayloadBytes(n int) Option { return func(c *openConfig) { c.maxPayloadBytes = n } }

// WithAppendNotify opts the Postgres append path into emitting a
// pg_notify hint per append (ADR 0039 C2). It is OFF by default and
// deliberately opt-in: with it unset the append path issues no
// additional statement and takes no additional lock. With it set, every
// non-empty append transaction runs
//
//	SELECT pg_notify($1, $2)
//
// as its last statement — after the events, the log_order row and the
// claims — with the channel and the workspace ID as parameters, so an
// idle feed subscriber can be woken in single-digit milliseconds on a
// clean fence instead of after a poll. NOTIFY is transactional: a
// rolled-back append sends nothing, a zero-event append (no events)
// sends nothing, and an AppendTx notifies exactly when the caller's
// transaction commits.
//
// channel must be a strict PostgreSQL identifier — non-empty, at most 63
// bytes (NAMEDATALEN−1), starting with a letter or underscore, then
// letters, digits and underscores — and both Open and OpenWithPool
// reject anything else before connecting. The channel name is a
// capability: choose a high-entropy unguessable name, never log it, and
// enable C2 only when every role that can CONNECT to the database is
// trusted or the database is dedicated (LISTEN and pg_notify are not
// RLS-filtered — see docs/deploy/README.md for the precondition and the
// operating model).
//
// Costs, stated plainly: one extra statement and round trip per
// non-empty append, plus the commit-time notify-queue lock that briefly
// serialises committing notifiers; a transaction that executed NOTIFY
// cannot be prepared for two-phase commit; and an exhausted async
// notification queue fails the append's COMMIT — the runbook
// operationalises exactly these (pg_notification_queue_usage()
// alerting, 2PC exclusion, when to prefer the application hint). A
// workspace ID over 512 bytes is rejected on the append path, so the
// 8000-byte NOTIFY payload limit is unreachable.
func WithAppendNotify(channel string) Option {
	return func(c *openConfig) { c.notifyChannel = channel }
}

// WithCheckpointNotify opts the Postgres checkpoint store into emitting a
// pg_notify hint on each committed Save (ADR 0039 C2). The checkpoint save
// transaction runs SELECT pg_notify(channel, scope) after the upsert, so the
// hint commits with the checkpoint and a rolled-back save notifies nobody.
// The payload is the scope (the workspace id), never the projection or a
// position: a waiter re-reads its own status and decides.
//
// It is OFF by default and deliberately opt-in; with it unset the save path
// takes no additional statement and no additional lock. The channel rules are
// exactly WithAppendNotify's — a strict PostgreSQL identifier, a high-entropy
// capability, never logged, enabled only under the same trust precondition,
// and unusable through PgBouncer transaction pooling on the LISTEN side. It
// must differ from the append channel. A scope over 512 bytes fails the save
// before it starts, and an exhausted async notification queue fails the save's
// commit; alert on pg_notification_queue_usage() as for the append channel.
func WithCheckpointNotify(channel string) Option {
	return func(c *openConfig) { c.markerNotifyChannel = channel }
}

// validate fails fast on options that cannot work, so a misconfigured
// Open surfaces here — before any connection, probe or migration —
// instead of on the first append.
func (c openConfig) validate() error {
	// An empty notify channel is the option being off, not a
	// misconfiguration; anything set must be a usable identifier.
	if c.notifyChannel != "" {
		if err := validNotifyChannel("WithAppendNotify", c.notifyChannel); err != nil {
			return err
		}
	}
	if c.markerNotifyChannel != "" {
		if err := validNotifyChannel("WithCheckpointNotify", c.markerNotifyChannel); err != nil {
			return err
		}
	}
	if c.notifyChannel != "" && c.notifyChannel == c.markerNotifyChannel {
		return fmt.Errorf("postgres: WithCheckpointNotify: channel must differ from WithAppendNotify's: the two wakes mean different things")
	}
	if err := validLedgerIdentifier("WithModule", c.module); err != nil {
		return err
	}
	return validLedgerIdentifier("WithLedgerSchema", c.ledgerSchema)
}

// validLedgerIdentifier applies the same conservative ASCII-identifier rule
// as the notify channel to the two ledger identifiers. Both reach SQL as
// text (CREATE SCHEMA cannot bind an identifier), and both are validated
// before any connection is opened.
func validLedgerIdentifier(option, name string) error {
	if name == "" {
		return fmt.Errorf("postgres: %s: identifier must not be empty", option)
	}
	if len(name) > maxNotifyChannelBytes {
		return fmt.Errorf("postgres: %s: identifier %q is %d bytes, over the %d-byte identifier limit", option, name, len(name), maxNotifyChannelBytes)
	}
	if !isIdentStart(name[0]) {
		return fmt.Errorf("postgres: %s: identifier %q is not a valid identifier: it must start with a letter or underscore", option, name)
	}
	for i := 1; i < len(name); i++ {
		if !isIdentPart(name[i]) {
			return fmt.Errorf("postgres: %s: identifier %q is not a valid identifier: only letters, digits and underscores are allowed", option, name)
		}
	}
	return nil
}

// maxNotifyChannelBytes is the longest notify channel accepted: 63
// bytes, NAMEDATALEN−1, the identifier limit PostgreSQL stores without
// silent truncation.
const maxNotifyChannelBytes = 63

// validNotifyChannel reports whether channel is a strict PostgreSQL
// identifier: non-empty, at most maxNotifyChannelBytes long, starting
// with a letter or underscore, then letters, digits and underscores. A
// keyword like "select" passes — LISTEN quotes the channel with
// pgx.Identifier.Sanitize — but the conservative ASCII-identifier rule
// keeps the name predictable for operators and impossible to smuggle
// syntax through. Both constructors run this at Open time, and the
// NotifyListener (ADR 0039 C2) reuses it, so one validator decides
// everywhere. The error names the channel — Open-time feedback on a
// misconfiguration; no other path ever puts it in a message or a log.
func validNotifyChannel(option, s string) error {
	if s == "" {
		return fmt.Errorf("postgres: %s: channel must not be empty", option)
	}
	if len(s) > maxNotifyChannelBytes {
		return fmt.Errorf("postgres: %s: channel %q is %d bytes, over the %d-byte identifier limit", option, s, len(s), maxNotifyChannelBytes)
	}
	if !isIdentStart(s[0]) {
		return fmt.Errorf("postgres: %s: channel %q is not a valid identifier: it must start with a letter or underscore", option, s)
	}
	for i := 1; i < len(s); i++ {
		if !isIdentPart(s[i]) {
			return fmt.Errorf("postgres: %s: channel %q is not a valid identifier: only letters, digits and underscores are allowed", option, s)
		}
	}
	return nil
}

func isIdentStart(b byte) bool {
	return b == '_' || 'a' <= b && b <= 'z' || 'A' <= b && b <= 'Z'
}

func isIdentPart(b byte) bool {
	return isIdentStart(b) || '0' <= b && b <= '9'
}

func newOpenConfig(opts []Option) openConfig {
	c := openConfig{
		autoMigrate:     true,
		maxPayloadBytes: defaultMaxPayloadBytes,
		module:          defaultLedgerModule,
		ledgerSchema:    defaultLedgerSchema,
	}
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

	// notifyChannel is the opt-in pg_notify channel (WithAppendNotify,
	// ADR 0039 C2). Empty — the default — means the append path emits no
	// hint statement at all. It is a capability: never logged, never
	// echoed into an error outside Open-time validation.
	notifyChannel string

	// markerNotifyChannel is the opt-in pg_notify channel for checkpoint
	// saves (WithCheckpointNotify, ADR 0039 C2). Empty means no hint. It is
	// a separate channel from notifyChannel so a marker wake cannot be
	// mistaken for an append wake, and it is a capability like the other.
	markerNotifyChannel string

	// module and ledgerSchema identify this store's rows in the shared
	// migration ledger. The ledger lives in the database the DSN names,
	// outside the event-store schema, so a schema reset cannot erase it.
	module       string
	ledgerSchema string

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
	// Fail fast: options are validated before any connection is opened,
	// so a misconfigured Open (a bad notify channel) surfaces as this
	// error — not as a first-append failure or a confusing connect error.
	cfg := newOpenConfig(opts)
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	if err := probePostgresFloor(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	s := &Store{
		pool:                pool,
		ownsPool:            true,
		maxPayloadBytes:     cfg.maxPayloadBytes,
		notifyChannel:       cfg.notifyChannel,
		markerNotifyChannel: cfg.markerNotifyChannel,
		module:              cfg.module,
		ledgerSchema:        cfg.ledgerSchema,
	}
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
	// Fail fast like Open: a misconfigured option (a bad notify channel)
	// is rejected before the version probe, which would otherwise be a
	// round trip wasted on a doomed Open.
	cfg := newOpenConfig(opts)
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if err := probePostgresFloor(ctx, pool); err != nil {
		return nil, err
	}
	s := &Store{
		pool:                pool,
		ownsPool:            false,
		maxPayloadBytes:     cfg.maxPayloadBytes,
		notifyChannel:       cfg.notifyChannel,
		markerNotifyChannel: cfg.markerNotifyChannel,
		module:              cfg.module,
		ledgerSchema:        cfg.ledgerSchema,
	}
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

// ledgerTable is the shared migration ledger as SQL: one table, outside the
// event-store schema, with a row per module and version.
func (s *Store) ledgerTable() string {
	return pgx.Identifier{s.ledgerSchema, "goose_migrations"}.Sanitize()
}

// ensureLedger creates the shared table only when it is physically absent.
// The schema is created only when it is absent too: CREATE ... IF NOT EXISTS
// checks the CREATE privilege even when the object exists, so a pre-created
// ledger owned by a maintenance role must not run DDL at all.
func (s *Store) ensureLedger(ctx context.Context, tx pgx.Tx) error {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`,
		s.ledgerSchema+".goose_migrations").Scan(&exists); err != nil {
		return fmt.Errorf("postgres: probe ledger: %w", err)
	}
	if exists {
		return nil
	}

	var schemaExists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = $1)`,
		s.ledgerSchema).Scan(&schemaExists); err != nil {
		return fmt.Errorf("postgres: probe ledger schema: %w", err)
	}
	if !schemaExists {
		if _, err := tx.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{s.ledgerSchema}.Sanitize()); err != nil {
			return fmt.Errorf("postgres: create ledger schema: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+s.ledgerTable()+` (
		module     text        NOT NULL,
		version    bigint      NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (module, version)
	)`); err != nil {
		return fmt.Errorf("postgres: create ledger: %w", err)
	}
	return nil
}

// adoptLegacyLedger carries a private schema_migrations history into the
// shared table exactly once. It runs under the migration lock and after
// ensureLedger, so a concurrent migrator cannot copy the same rows twice.
//
// The legacy table is unqualified on purpose: the connection's search_path
// is what makes it the event store's own history (eventstore.schema_migrations
// in a deployment that moved the store out of public, or public in an older
// one). A database that never had one is a fresh install, not an upgrade.
func (s *Store) adoptLegacyLedger(ctx context.Context, tx pgx.Tx) error {
	var hasRows bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM `+s.ledgerTable()+` WHERE module = $1)`,
		s.module).Scan(&hasRows); err != nil {
		return fmt.Errorf("postgres: check ledger rows: %w", err)
	}
	if hasRows {
		return nil
	}

	var legacy bool
	if err := tx.QueryRow(ctx, `SELECT to_regclass('schema_migrations') IS NOT NULL`).Scan(&legacy); err != nil {
		return fmt.Errorf("postgres: probe legacy ledger: %w", err)
	}
	if !legacy {
		return nil
	}

	if _, err := tx.Exec(ctx, `INSERT INTO `+s.ledgerTable()+` (module, version, applied_at)
		SELECT $1, version, applied_at FROM schema_migrations
		ON CONFLICT (module, version) DO NOTHING`, s.module); err != nil {
		return fmt.Errorf("postgres: adopt legacy ledger: %w", err)
	}
	return nil
}

// isDuplicateLedgerObject reports the catalog races a second process can
// lose while creating the ledger. The whole bootstrap transaction is retried
// rather than swallowing the error: a failed statement aborts the
// transaction, so continuing inside it would only produce a second error.
func isDuplicateLedgerObject(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	switch pgErr.Code {
	case "42P06", "42P07", "23505": // duplicate_schema, duplicate_table, unique_violation
		return true
	default:
		return false
	}
}

// bootstrapLedger creates or adopts the shared ledger inside the migration
// lock. A concurrent pgmigrate module can create the same schema and table,
// so a lost race is retried; the next attempt sees the winner's objects.
func (s *Store) bootstrapLedger(ctx context.Context) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		err = s.migrateInLock(ctx, func(tx pgx.Tx) error {
			if err := s.ensureLedger(ctx, tx); err != nil {
				return err
			}
			return s.adoptLegacyLedger(ctx, tx)
		})
		if err == nil || !isDuplicateLedgerObject(err) {
			return err
		}
	}
	return err
}

// Migrate applies pending versioned migrations (ADR 0010). Each pending
// migration runs in exactly one transaction — BEGIN; SELECT
// pg_advisory_xact_lock($1); re-check the recorded version; apply;
// record; COMMIT — and the shared-ledger bootstrap runs inside a
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
	// Create or adopt the shared ledger in its own xact-locked
	// transaction, shaped exactly like a migration's: concurrent
	// migrators may all reach this on a fresh database.
	if err := s.bootstrapLedger(ctx); err != nil {
		return fmt.Errorf("postgres: migration ledger: %w", err)
	}
	// The recorded versions, read once as a fast path: a version listed
	// here is applied, and a version missing here may since have been
	// applied by a concurrent migrator — which is why every version the
	// read marks pending is re-checked under the lock below. The read
	// can be stale only towards "pending", and a stale miss costs an
	// empty locked skip, never a double apply.
	rows, err := s.pool.Query(ctx,
		`SELECT version FROM `+s.ledgerTable()+` WHERE module = $1`, s.module)
	if err != nil {
		return fmt.Errorf("postgres: read ledger: %w", err)
	}
	defer rows.Close()
	applied := map[int]bool{}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return fmt.Errorf("postgres: read ledger: %w", err)
		}
		applied[v] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("postgres: read ledger: %w", err)
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
				`SELECT EXISTS(SELECT 1 FROM `+s.ledgerTable()+` WHERE module = $1 AND version = $2)`,
				s.module, m.Version).Scan(&exists); err != nil {
				return fmt.Errorf("postgres: check migration %d: %w", m.Version, err)
			}
			if exists {
				return nil
			}
			if _, err := tx.Exec(ctx, m.SQL); err != nil {
				return fmt.Errorf("postgres: migration %04d_%s: %w", m.Version, m.Name, err)
			}
			if _, err := tx.Exec(ctx,
				`INSERT INTO `+s.ledgerTable()+` (module, version) VALUES ($1, $2)`,
				s.module, m.Version); err != nil {
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
	actor_type, actor_id, payload, workspace_id, prev_hash, hash,
	trace_id, span_id, trace_flags,
	via_type, via_id, via_principal, constraint_ops, tags`

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
		traceID, spanID                       []byte
		flags                                 *int16
		viaType, viaID, viaPrincipal          *string
		constraintOps                         []byte
		tagsRaw                               []byte
	)
	if err := row.Scan(
		&e.GlobalPosition, &eventID, &streamType, &streamID, &e.Version,
		&e.TypeURL, &e.SchemaVersion, &e.OccurredAt, &e.RecordedAt,
		&correlationID, &causationID, &commandID,
		&actorType, &actorID, &payload, &workspaceID, &e.PrevHash, &e.Hash,
		&traceID, &spanID, &flags,
		&viaType, &viaID, &viaPrincipal, &constraintOps, &tagsRaw,
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
	// Constraint ops (ADR 0043) are a restore record, not chain-attested:
	// a corrupt blob fails the read loudly — naming the stream and version,
	// never the bytes — rather than silently dropping the claims it would
	// reconstruct. Decode whenever the column is non-NULL, so an out-of-band
	// zero-length bytea fails as corrupt instead of reading as "no ops"; the
	// store itself never writes one.
	if constraintOps != nil {
		if e.ConstraintOps, err = es.DecodeConstraintOps(constraintOps); err != nil {
			return es.Envelope{}, fmt.Errorf("constraint_ops for stream %s v%d: %w", sid, e.Version, err)
		}
	}
	// Tags are indexed metadata, not chain-attested: a stored value that is
	// not the flat string map the append wrote is corruption and fails the
	// read loudly — naming the stream and version, never the bytes — rather
	// than silently dropping the tags a change feed depends on.
	if e.Tags, err = es.DecodeTags(tagsRaw); err != nil {
		return es.Envelope{}, fmt.Errorf("tags for stream %s v%d: %w", sid, e.Version, err)
	}
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
	// The delegated actor (ADR 0041) is an audit fact like the UUIDs
	// above, and is reconstructed strictly: a row carrying only some of
	// the three columns fails the read instead of silently reading as a
	// zero Via, which would forge an actor that acted for itself.
	if e.Actor.Via, err = decodeVia(viaType, viaID, viaPrincipal); err != nil {
		return es.Envelope{}, err
	}
	e.Payload = payload
	// The span context is the one deliberate exception to the
	// load-bearing-UUID rule above: a stored value that does not parse
	// reads as absent, never a read error (ADR 0040 §4). The decode is
	// total and unit-tested against shapes the CHECK keeps out of the
	// store.
	e.SpanContext = decodeSpanContext(traceID, spanID, flags)
	return e, nil
}

// decodeVia reconstructs the delegated actor (ADR 0041) from the three
// stored columns — the via_type, via_id and via_principal text values,
// each nil for NULL — into an es.ActorRef. Via is an audit fact, the
// deliberate opposite of the span context's telemetry tolerance above:
// a row with only some of the three set is a partially recorded
// delegation and fails the read, naming the NULL columns, instead of
// silently reading as a zero Via that would forge an actor acting for
// itself (ADR 0041 §4). The events_via_complete CHECK keeps every such
// shape out of the store; this strictness is the second line of
// defence, for a restored dump or a constraint bypassed during a
// migration. Three NULLs read as the zero ref; three values read as
// the ref they record (via_principal is the derived "type:id" form the
// via index keys on and is not re-derived or cross-checked here).
func decodeVia(viaType, viaID, viaPrincipal *string) (es.ActorRef, error) {
	switch {
	case viaType == nil && viaID == nil && viaPrincipal == nil:
		return es.ActorRef{}, nil
	case viaType != nil && viaID != nil && viaPrincipal != nil:
		return es.ActorRef{Type: *viaType, ID: *viaID}, nil
	default:
		var missing []string
		if viaType == nil {
			missing = append(missing, "via_type")
		}
		if viaID == nil {
			missing = append(missing, "via_id")
		}
		if viaPrincipal == nil {
			missing = append(missing, "via_principal")
		}
		return es.ActorRef{}, fmt.Errorf("stored delegation is incomplete: %s NULL with the rest set", strings.Join(missing, ", "))
	}
}

// decodeSpanContext decodes the three stored span-context columns — the
// raw trace_id and span_id bytes and the smallint trace_flags — into an
// es.SpanContext, never an error: telemetry must never fail a read
// (ADR 0040 §4). The events_span_context_complete CHECK makes every
// malformed shape below unreachable from the store; this tolerance is
// the second line of defence, for a restored dump or a constraint
// bypassed during a migration.
//
// NULL trace_flags reads as absent: the CHECK ties NULL flags to NULL
// ids, so ids alongside a NULL flags column are dump corruption, and
// the row recorded no context to trust them against. An out-of-range
// flags reads as absent too — narrowing it would fabricate a context
// (300 would parse as 44). Otherwise ParseSpanContext decides, and
// whatever it rejects (wrong-length or all-zero ids, a half context)
// reads as absent as well.
func decodeSpanContext(traceID, spanID []byte, flags *int16) es.SpanContext {
	if flags == nil {
		return es.SpanContext{}
	}
	if *flags < 0 || *flags > 255 {
		return es.SpanContext{}
	}
	sc, err := es.ParseSpanContext(traceID, spanID, uint8(*flags))
	if err != nil {
		return es.SpanContext{}
	}
	return sc
}
