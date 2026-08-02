// Package postgres is the Postgres backend for es-lite: shared tables with
// per-workspace RLS and hash partitioning (ADR 0004), payloads encrypted at
// rest under per-workspace DEKs (ADR 0004 / shred), and a gap-safe delivery
// claim-drain (ADR 0001 §5).
//
// Usage:
//
//	store, _ := postgres.Open(ctx, dsn, keyStore)   // keyStore may be nil (plaintext)
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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/keystore"
	"github.com/laenenai/es-lite/migrate"
	"github.com/laenenai/es-lite/shred"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// migrateLockKey is the fixed advisory-lock key held while migrating, so
// concurrent migrators serialize (ADR 0010).
const migrateLockKey int64 = 0x65736C697465 // "eslite"

// Option configures Open.
type Option func(*openConfig)

type openConfig struct{ autoMigrate bool }

// WithoutAutoMigrate skips applying migrations on Open. Application replicas
// use this and rely on a separate Migrate step / init container (ADR 0010);
// the default auto-migrates so dev/single-node stays zero-config.
func WithoutAutoMigrate() Option { return func(c *openConfig) { c.autoMigrate = false } }

func newOpenConfig(opts []Option) openConfig {
	c := openConfig{autoMigrate: true}
	for _, o := range opts {
		o(&c)
	}
	return c
}

// Store is the Postgres-backed event store. Construct per process; it holds
// a connection pool and (optionally) the crypto-shredding layer.
type Store struct {
	pool     *pgxpool.Pool
	shredder *shred.Shredder // nil => payloads stored/returned as plaintext
}

// Open connects to Postgres, applies migrations (unless WithoutAutoMigrate),
// and — when ks is non-nil — wires per-workspace crypto-shredding, using this
// store's workspace_keys table as the wrapped-DEK store.
func Open(ctx context.Context, dsn string, ks keystore.KeyStore, opts ...Option) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	s := &Store{pool: pool}
	if ks != nil {
		s.shredder = shred.New(ks, s) // Store is its own WrappedDEKStore
	}
	if newOpenConfig(opts).autoMigrate {
		if err := s.Migrate(ctx); err != nil {
			pool.Close()
			return nil, err
		}
	}
	return s, nil
}

// Migrate applies pending versioned migrations under a session advisory lock,
// so concurrent migrators are safe (ADR 0010). Each migration runs in its own
// transaction and is recorded in schema_migrations. Idempotent.
func (s *Store) Migrate(ctx context.Context) error {
	migs, err := migrate.Load(migrationsFS, "migrations")
	if err != nil {
		return err
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("postgres: acquire: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrateLockKey); err != nil {
		return fmt.Errorf("postgres: advisory lock: %w", err)
	}
	defer conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, migrateLockKey)

	if _, err := conn.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS schema_migrations (
			version    int         PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("postgres: schema_migrations: %w", err)
	}
	for _, m := range migs {
		var exists bool
		if err := conn.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, m.Version).Scan(&exists); err != nil {
			return fmt.Errorf("postgres: check migration %d: %w", m.Version, err)
		}
		if exists {
			continue
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, m.SQL); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("postgres: migration %04d_%s: %w", m.Version, m.Name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, m.Version); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("postgres: record migration %d: %w", m.Version, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("postgres: commit migration %d: %w", m.Version, err)
		}
	}
	return nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// Pool exposes the underlying pool (for read-model projections colocated in
// the same database).
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Workspace returns an es.Store scoped to one workspace: every operation runs
// in a transaction that sets the RLS variable and encrypts/decrypts payloads
// with the workspace DEK.
func (s *Store) Workspace(workspaceID string) es.Store {
	return &wsStore{store: s, ws: workspaceID}
}

// ForgetWorkspace crypto-shreds a workspace (destroys its KEK, drops its
// wrapped DEK). The event rows stay (immutable, now-unreadable ciphertext);
// the caller must also purge the workspace's projections and NATS-retained
// payloads (ADR 0004 §4). No-op when encryption is disabled.
func (s *Store) ForgetWorkspace(ctx context.Context, workspaceID string) error {
	if s.shredder == nil {
		return nil
	}
	return s.shredder.Forget(ctx, workspaceID)
}

// ---- shred.WrappedDEKStore (workspace_keys) --------------------------------

func (s *Store) LoadWrappedDEK(ctx context.Context, workspaceID string) ([]byte, bool, error) {
	var w []byte
	err := s.pool.QueryRow(ctx,
		`SELECT wrapped_dek FROM workspace_keys WHERE workspace_id = $1`, workspaceID,
	).Scan(&w)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return w, true, nil
}

func (s *Store) SaveWrappedDEK(ctx context.Context, workspaceID string, wrapped []byte, kekVersion int) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO workspace_keys (workspace_id, wrapped_dek, kek_version)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (workspace_id) DO NOTHING`,
		workspaceID, wrapped, kekVersion)
	return err
}

func (s *Store) DeleteWrappedDEK(ctx context.Context, workspaceID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM workspace_keys WHERE workspace_id = $1`, workspaceID)
	return err
}

// writeCipher returns the workspace Cipher for the write path (provisions a
// DEK on first use), or nil when encryption is disabled.
func (s *Store) writeCipher(ctx context.Context, ws string) (*shred.Cipher, error) {
	if s.shredder == nil {
		return nil, nil
	}
	c, err := s.shredder.WriteCipher(ctx, ws)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// readCipher returns the workspace Cipher for the read path (ErrShredded if
// the workspace was forgotten), or nil when encryption is disabled.
func (s *Store) readCipher(ctx context.Context, ws string) (*shred.Cipher, error) {
	if s.shredder == nil {
		return nil, nil
	}
	c, err := s.shredder.ReadCipher(ctx, ws)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

func (s *Store) encrypt(c *shred.Cipher, plaintext []byte) ([]byte, error) {
	if c == nil {
		return plaintext, nil
	}
	return c.Encrypt(plaintext)
}

func (s *Store) decrypt(c *shred.Cipher, blob []byte) ([]byte, error) {
	if c == nil {
		return blob, nil
	}
	return c.Decrypt(blob)
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
	actor_type, actor_id, payload, workspace_id`

// scanEnvelope reads one row (in envelopeCols order) WITHOUT decrypting,
// returning the still-encrypted payload separately so callers can decrypt
// with the right workspace cipher only when there is data to decrypt.
func scanEnvelope(row pgx.Row) (es.Envelope, []byte, error) {
	var (
		e                                     es.Envelope
		eventID, streamType, streamID         string
		correlationID, causationID, commandID string
		actorType, actorID                    string
		ciphertext                            []byte
		workspaceID                           string
	)
	if err := row.Scan(
		&e.GlobalPosition, &eventID, &streamType, &streamID, &e.Version,
		&e.TypeURL, &e.SchemaVersion, &e.OccurredAt, &e.RecordedAt,
		&correlationID, &causationID, &commandID,
		&actorType, &actorID, &ciphertext, &workspaceID,
	); err != nil {
		return es.Envelope{}, nil, err
	}
	sid, err := es.ParseCanonical(streamID)
	if err != nil {
		return es.Envelope{}, nil, fmt.Errorf("parse stream_id %q: %w", streamID, err)
	}
	e.StreamID = sid
	e.Workspace = workspaceID
	e.EventID, _ = uuid.Parse(eventID)
	e.CorrelationID, _ = uuid.Parse(correlationID)
	e.CausationID, _ = uuid.Parse(causationID)
	e.CommandID, _ = uuid.Parse(commandID)
	e.Actor = es.Actor{Type: actorType, ID: actorID}
	return e, ciphertext, nil
}
