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
	if err := validateAppend(p); err != nil {
		return zero, err
	}
	if err := s.checkPayload(p); err != nil {
		return zero, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return zero, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	res, err := s.appendTx(ctx, tx, p)
	if err != nil {
		return zero, err
	}
	if err := tx.Commit(); err != nil {
		return zero, fmt.Errorf("commit: %w", err)
	}
	return res, nil
}

// checkPayload rejects the whole batch before the transaction is even opened —
// nothing is inserted, no position drawn. A single oversized event otherwise
// wedges every feed subscription consuming its type. The error carries stream,
// version, type URL and size only — never the payload bytes.
func (s *Store) checkPayload(p es.AppendParams) error {
	for i, ev := range p.Events {
		if s.maxPayloadBytes > 0 && len(ev.Payload) > s.maxPayloadBytes {
			version := p.ExpectedVersion + uint64(i) + 1
			return fmt.Errorf("%w: stream %s v%d, type %s, payload %d bytes exceeds cap %d",
				es.ErrPayloadTooLarge, p.StreamID, version, ev.TypeURL, len(ev.Payload), s.maxPayloadBytes)
		}
	}
	return nil
}

// appendTx is the shared append body. The caller has validated p and owns the
// transaction boundary.
func (s *Store) appendTx(ctx context.Context, tx *sql.Tx, p es.AppendParams) (es.AppendResult, error) {
	var zero es.AppendResult

	// Timestamps are truncated to microseconds — the precision the hash chain
	// attests and every backend returns (ADR 0029 §2).
	occurred := p.OccurredAt
	if occurred.IsZero() {
		occurred = time.Now()
	}
	occurred = es.TruncateMicros(occurred)

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
	// Validate and merge the tag set once, before any insert.
	merged, err := mergedTags(p)
	if err != nil {
		return zero, err
	}
	// The span context is a property of the append (the producing trace),
	// not of an individual event, so one mapping covers the whole batch.
	traceID, spanID, flags := spanContextArgs(p.SpanContext)
	// The delegated actor (ADR 0041) is likewise a property of the
	// append — the actor acting through the service — so one mapping
	// covers the batch too.
	viaType, viaID, viaPrincipal := viaArgs(p.Actor.Via)
	// Constraint ops are a batch-level record (ADR 0043): encode once and
	// write the blob on the FIRST event only; every later row is NULL. The
	// ops are excluded from the hash (CanonicalEnvelope), so this changes
	// no hash byte.
	var encodedOps []byte
	if len(p.Constraints) > 0 {
		encodedOps = es.EncodeConstraintOps(p.Constraints)
	}
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
			SpanContext:   p.SpanContext,
			Tags:          merged[i],
			Payload:       ev.Payload,
			PrevHash:      append([]byte(nil), prev[:]...), // copy: prev is rewritten below
		}
		var opsArg []byte
		if i == 0 && len(encodedOps) > 0 {
			opsArg = encodedOps
			env.ConstraintOps = append([]es.ConstraintOp(nil), p.Constraints...)
		}
		var tagsArg any
		if len(merged[i]) > 0 {
			raw, err := es.EncodeTags(merged[i])
			if err != nil {
				return zero, err
			}
			tagsArg = string(raw)
		}
		hash := es.EnvelopeHash(env)
		env.Hash = hash[:]
		prev = hash // within one batch, event i chains to event i-1 in memory

		res, err := tx.ExecContext(ctx,
			`INSERT INTO events (
				event_id, stream_type, stream_id, version,
				type_url, schema_version, occurred_at, recorded_at,
				correlation_id, causation_id, command_id,
				actor_type, actor_id, actor_principal,
				via_type, via_id, via_principal, payload,
				prev_hash, hash,
				trace_id, span_id, trace_flags, constraint_ops, tags
			) VALUES (?,?,?,?, ?,?,?,?, ?,?,?, ?,?,?, ?,?,?,?, ?,?, ?,?,?,?,?)`,
			eventID.String(), p.StreamID.Type, canonical, version,
			ev.TypeURL, ev.SchemaVersion, formatTS(occurred), formatTS(recorded),
			p.CorrelationID.String(), p.CausationID.String(), p.CommandID.String(),
			p.Actor.Type, p.Actor.ID, p.Actor.Principal(),
			viaType, viaID, viaPrincipal, ev.Payload,
			env.PrevHash, env.Hash,
			traceID, spanID, flags, opsArg, tagsArg,
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

	// Event-tag projection: one multi-row insert per append, in the same
	// transaction, from the same merged sets the events wrote.
	var tagPlaceholders []string
	var tagArgs []any
	var tagEntries []es.TagEntry
	for i := range envs {
		for k, v := range merged[i] {
			tagPlaceholders = append(tagPlaceholders, "(?,?,?)")
			tagArgs = append(tagArgs, k, v, int64(envs[i].GlobalPosition))
			tagEntries = append(tagEntries, es.TagEntry{
				Position: envs[i].GlobalPosition,
				Key:      k,
				Value:    v,
			})
		}
	}
	if len(tagPlaceholders) > 0 {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO event_tags (tag_key, tag_value, global_position) VALUES `+strings.Join(tagPlaceholders, ","),
			tagArgs...,
		); err != nil {
			return zero, fmt.Errorf("insert event_tags: %w", err)
		}
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
		var tagsHashArg []byte
		if len(tagEntries) > 0 {
			h := es.TagsHash(tagEntries)
			tagsHashArg = h[:]
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO log_order (from_position, to_position, stream_id, event_count, batch_hash, tags_hash)
			 VALUES (?,?,?,?,?,?)`,
			envs[0].GlobalPosition, envs[len(envs)-1].GlobalPosition,
			canonical, len(envs), batchHash[:], tagsHashArg,
		); err != nil {
			return zero, fmt.Errorf("insert log_order: %w", err)
		}
	}

	if err := applyClaims(ctx, tx, "unique_claims", canonical, p.Constraints); err != nil {
		return zero, err
	}

	var position uint64
	if len(envs) > 0 {
		position = envs[len(envs)-1].GlobalPosition
	}
	return es.AppendResult{
		FromVersion: p.ExpectedVersion,
		ToVersion:   p.ExpectedVersion + uint64(len(p.Events)),
		Envelopes:   envs,
		Position:    position,
	}, nil
}

// AppendMulti appends several streams in one store-owned transaction. SQLite
// has no caller-owned append transaction, so this is the multi-stream entry
// point; the contract is es.MultiAppend's. The appends run in input order under
// one resolved OccurredAt, and any failure rolls the whole set back.
func (s *Store) AppendMulti(ctx context.Context, m es.MultiAppend) ([]es.AppendResult, error) {
	if err := es.ValidateMultiAppend(m); err != nil {
		return nil, err
	}
	for i := range m.Appends {
		if err := validateAppend(m.Appends[i]); err != nil {
			return nil, err
		}
		if err := s.checkPayload(m.Appends[i]); err != nil {
			return nil, err
		}
	}

	occurred := es.MultiOccurredAt(m.Appends, time.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	out := make([]es.AppendResult, len(m.Appends))
	for i := range m.Appends {
		p := m.Appends[i]
		p.OccurredAt = occurred
		res, err := s.appendTx(ctx, tx, p)
		if err != nil {
			return nil, err
		}
		out[i] = res
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return out, nil
}

// mergedTags returns one merged, validated tag set per event: the append's
// batch tags overlaid with each event's own. validateAppend calls it before
// anything is written; Append calls it again to project event_tags and hash
// the batch.
func mergedTags(p es.AppendParams) ([]map[string]string, error) {
	per := make([]map[string]string, len(p.Events))
	for i := range p.Events {
		per[i] = p.Events[i].Tags
	}
	return es.MergedTags(p.Tags, per)
}

// validateAppend applies the append-time invariants shared by every append
// path before any statement runs. The error names the stream where the rule is
// stream-scoped.
func validateAppend(p es.AppendParams) error {
	if err := p.StreamID.Validate(); err != nil {
		return err
	}
	// Tags ride on events, like constraint ops: a tag with no event would be
	// silently dropped, so refuse it before the transaction opens.
	if len(p.Events) == 0 && len(p.Tags) > 0 {
		return fmt.Errorf("%w: tags require at least one event: %s", es.ErrInvalidTag, p.StreamID)
	}
	if _, err := mergedTags(p); err != nil {
		return err
	}
	for _, op := range p.Constraints {
		if err := op.Validate(); err != nil {
			return err
		}
	}
	// A partial span context (ADR 0040) is refused before anything runs:
	// the zero value means "absent" and is fine, but a caller that sets
	// one id and not the other has produced a malformed context, and the
	// events table's CHECK would abort the whole insert anyway. The
	// error names the stream.
	if p.SpanContext != (es.SpanContext{}) && !p.SpanContext.IsValid() {
		return fmt.Errorf("%w: %s", es.ErrMalformedSpanContext, p.StreamID)
	}
	// The delegated actor (ADR 0041 §3.4) is refused the same way, on
	// the same terms: Actor.Validate enforces the delegation rules — a
	// Via must be complete and must accompany an accountable actor —
	// and a rejection means nothing was written. There is no CHECK
	// behind it here (ADR 0041 §3.3: enforcing one needs another events
	// rebuild), so this call is the all-or-nothing enforcement.
	if err := p.Actor.Validate(); err != nil {
		return fmt.Errorf("%w: %s", err, p.StreamID)
	}
	// Constraint ops ride on the first event of the batch (ADR 0043): an
	// append that carries ops but no event would be invisible state — a
	// claim with no log row to reconstruct it. Refuse it before the
	// transaction opens, so nothing is written.
	if len(p.Events) == 0 && len(p.Constraints) > 0 {
		return fmt.Errorf("%w: %s", es.ErrConstraintWithoutEvent, p.StreamID)
	}
	return nil
}

// spanContextArgs maps a SpanContext onto the events table's three
// nullable columns (trace_id, span_id, trace_flags): the zero value —
// "absent" (ADR 0040) — writes NULL, NULL, NULL, and a populated one
// writes both ids as BLOBs and the flags byte as an integer. Append has
// already rejected non-zero invalid values, so the populated arm only
// ever sees a complete context. Deliberately duplicated from the
// Postgres adapter rather than shared through es (plan ruling R1): a
// six-line helper twice beats putting SQL NULL semantics in the domain
// package.
func spanContextArgs(sc es.SpanContext) (traceID, spanID []byte, flags *int16) {
	if sc == (es.SpanContext{}) {
		return nil, nil, nil
	}
	f := int16(sc.Flags)
	return sc.TraceID[:], sc.SpanID[:], &f
}

// viaArgs maps a delegated actor (ADR 0041) onto the events table's
// three nullable columns (via_type, via_id, via_principal): the zero
// ref — "not delegated" — writes NULL, NULL, NULL, and a populated one
// writes the type, the id and the canonical "type:id" principal the
// via index keys on. Append has already rejected non-zero invalid
// values, so the populated arm only ever sees a complete ref — and
// unlike Postgres there is no CHECK behind it (ADR 0041 §3.3), which
// makes the validation above the all-or-nothing enforcement. Like
// spanContextArgs, deliberately duplicated from the Postgres adapter
// rather than shared through es (plan ruling R1): a six-line helper
// twice beats putting SQL NULL semantics in the domain package.
func viaArgs(ref es.ActorRef) (viaType, viaID, viaPrincipal *string) {
	if ref.IsZero() {
		return nil, nil, nil
	}
	t, id, principal := ref.Type, ref.ID, ref.Principal()
	return &t, &id, &principal
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
// (ADR 0025). table names the index to update; the append path passes
// "unique_claims", and a rebuild (ADR 0043) replays into a temporary table.
//
// No ordering lock is needed: a constraint-bearing append carries at least
// one event (validateAppend), so same-stream appends serialize behind the
// single SQLite writer, and every op sequence has a stable global position to
// replay in.
func applyClaims(ctx context.Context, tx *sql.Tx, table, streamID string, ops []es.ConstraintOp) error {
	for _, op := range ops {
		if op.Op != es.ReleaseOp {
			continue
		}
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM `+table+` WHERE scope = ? AND value_key = ? AND stream_id = ?`,
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
			`INSERT INTO `+table+` (scope, value_key, stream_id) VALUES (?, ?, ?)`,
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
	actor_type, actor_id, payload, prev_hash, hash,
	trace_id, span_id, trace_flags,
	via_type, via_id, via_principal, constraint_ops, tags`

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
// read rather than being silently zeroed into a forged envelope. The
// delegated actor (ADR 0041) is reconstructed just as strictly — a
// half-recorded delegation fails the read. The span context is the one
// deliberate exception: a value that does not parse reads as absent,
// never a read error (ADR 0040 §4).
func scanEnvelope(rows *sql.Rows) (es.Envelope, error) {
	var (
		e                                     es.Envelope
		eventID, streamType, streamID         string
		occurredAt, recordedAt                string
		correlationID, causationID, commandID string
		actorType, actorID                    string
		traceID, spanID                       []byte
		flags                                 any
		viaType, viaID, viaPrincipal          *string
		// sql.Null distinguishes a NULL column from a zero-length blob: the
		// driver scans both as a nil []byte, so the slice alone cannot.
		constraintOps sql.Null[[]byte]
		tagsRaw       sql.Null[string]
	)
	if err := rows.Scan(
		&e.GlobalPosition, &eventID, &streamType, &streamID, &e.Version,
		&e.TypeURL, &e.SchemaVersion, &occurredAt, &recordedAt,
		&correlationID, &causationID, &commandID,
		&actorType, &actorID, &e.Payload, &e.PrevHash, &e.Hash,
		&traceID, &spanID, &flags,
		&viaType, &viaID, &viaPrincipal, &constraintOps, &tagsRaw,
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
	// Constraint ops (ADR 0043) are a restore record, not chain-attested: a
	// corrupt blob fails the read loudly — naming the stream and version,
	// never the bytes — rather than silently dropping the claims it would
	// reconstruct. Decode whenever the column is non-NULL, so an out-of-band
	// zero-length blob fails as corrupt instead of reading as "no ops"; the
	// store itself never writes one.
	if constraintOps.Valid {
		if e.ConstraintOps, err = es.DecodeConstraintOps(constraintOps.V); err != nil {
			return es.Envelope{}, fmt.Errorf("constraint_ops for stream %s v%d: %w", e.StreamID, e.Version, err)
		}
	}
	// Tags are indexed metadata, not chain-attested: a stored value that is
	// not the flat string map the append wrote is corruption and fails the
	// read loudly, rather than silently dropping the tags a feed depends on.
	// A NULL column is the one representation of "no tags"; any non-NULL
	// value must be an object (a literal "null" or empty string is corrupt).
	if tagsRaw.Valid {
		if e.Tags, err = es.DecodeStoredTags([]byte(tagsRaw.V)); err != nil {
			return es.Envelope{}, fmt.Errorf("tags for stream %s v%d: %w", e.StreamID, e.Version, err)
		}
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
	// The delegated actor (ADR 0041) is an audit fact like the UUIDs
	// above, and is reconstructed strictly: a row carrying only some of
	// the three columns fails the read instead of silently reading as a
	// zero Via, which would forge an actor that acted for itself.
	if e.Actor.Via, err = decodeVia(viaType, viaID, viaPrincipal); err != nil {
		return es.Envelope{}, err
	}
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
// itself (ADR 0041 §3.3). Postgres backs this with an all-or-nothing
// CHECK; SQLite has none (ADR 0041 §3.3: enforcing one needs another
// events rebuild), so this strict read is what keeps a restored dump or
// an out-of-band write honest. Three NULLs read as the zero ref; three
// values read as the ref they record (via_principal is the derived
// "type:id" form the via index keys on and is not re-derived or
// cross-checked here).
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
// raw trace_id and span_id bytes and trace_flags — into an
// es.SpanContext, never an error: telemetry must never fail a read
// (ADR 0040 §4). The rebuild migration's CHECK makes every malformed
// shape below unreachable from the store; this tolerance is the second
// line of defence, for a restored dump or a constraint bypassed during a
// migration.
//
// trace_flags is declared INTEGER, but SQLite's typing is dynamic and a
// restored dump can land any value in the column — the reason flags
// scans as any, which accepts every driver value, instead of a *int64,
// which fails the whole read on a TEXT value. Only an int64 in 0..255
// proceeds; NULL, every other type and every out-of-range integer read
// as absent (narrowing an out-of-range value would fabricate a context
// — 300 would parse as 44). Otherwise ParseSpanContext decides, and
// whatever it rejects (wrong-length or all-zero ids, a half context)
// reads as absent as well.
func decodeSpanContext(traceID, spanID []byte, flags any) es.SpanContext {
	var f uint8
	switch v := flags.(type) {
	case nil:
		return es.SpanContext{}
	case int64:
		if v < 0 || v > 255 {
			return es.SpanContext{}
		}
		f = uint8(v)
	default:
		return es.SpanContext{}
	}
	sc, err := es.ParseSpanContext(traceID, spanID, f)
	if err != nil {
		return es.SpanContext{}
	}
	return sc
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
