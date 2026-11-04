package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/laenenai/es-lite/es"
)

// DBTX is the narrow database handle the transaction-aware methods need:
// statement execution only, with no Commit, Rollback, Begin or pool. It is
// the shape sqlc uses for pgx/v5 and the shape tenancy.TenantTx exposes, so a
// tenant-scoped transaction satisfies it without es-lite importing tenancy.
// *pgxpool.Pool and pgx.Tx also satisfy it.
type DBTX interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Workspace is an es.Store scoped to one workspace. Every operation sets
// app.workspace_id (RLS) and, for partition pruning, filters by workspace_id
// explicitly. Payloads are stored and returned as opaque bytes (ADR 0025) —
// es-lite performs no encryption.
//
// Append/ReadStream open and own their transaction. AppendTx/ReadStreamTx
// join a transaction the caller already owns; see WorkspaceStore. Create one
// with Store.WorkspaceStore; the zero value is not usable.
type Workspace struct {
	store *Store
	ws    string
}

var _ es.Store = (*Workspace)(nil)

// setWorkspace binds the RLS variable for the current transaction.
// is_local=true scopes it to that transaction, so a caller's tenant context
// (app.tenant_id) is untouched and the setting cannot leak past commit.
func setWorkspace(ctx context.Context, db DBTX, ws string) error {
	// set_config(name, value, is_local=true) — parameterized, unlike SET.
	_, err := db.Exec(ctx, `SELECT set_config('app.workspace_id', $1, true)`, ws)
	return err
}

// Append implements es.Store. It opens and owns a transaction.
func (w *Workspace) Append(ctx context.Context, p es.AppendParams) (es.AppendResult, error) {
	var zero es.AppendResult
	if err := validateAppend(p); err != nil {
		return zero, err
	}
	tx, err := w.store.pool.Begin(ctx)
	if err != nil {
		return zero, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := setWorkspace(ctx, tx, w.ws); err != nil {
		return zero, fmt.Errorf("set workspace: %w", err)
	}
	res, err := w.append(ctx, tx, p)
	if err != nil {
		return zero, err
	}
	if err := tx.Commit(ctx); err != nil {
		return zero, fmt.Errorf("commit: %w", err)
	}
	return res, nil
}

// AppendTx appends inside a transaction the caller already owns. It never
// commits or rolls back that transaction. It sets app.workspace_id on db
// (is_local=true) so the workspace RLS policy applies for the rest of the
// transaction; the caller's tenant context is left untouched.
//
// db must be an active transaction, not a pool: the local setting lasts only
// until the transaction ends, and every statement in one append must share
// it. tenancy.TenantTx satisfies db directly, without a pool or Commit.
func (w *Workspace) AppendTx(ctx context.Context, db DBTX, p es.AppendParams) (es.AppendResult, error) {
	var zero es.AppendResult
	if err := validateAppend(p); err != nil {
		return zero, err
	}
	if err := setWorkspace(ctx, db, w.ws); err != nil {
		return zero, fmt.Errorf("set workspace: %w", err)
	}
	return w.append(ctx, db, p)
}

func validateAppend(p es.AppendParams) error {
	if err := p.StreamID.Validate(); err != nil {
		return err
	}
	for _, op := range p.Constraints {
		if err := op.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// append is the shared append body. The caller has validated p, set the
// workspace RLS context and owns the transaction boundary.
func (w *Workspace) append(ctx context.Context, db DBTX, p es.AppendParams) (es.AppendResult, error) {
	var zero es.AppendResult

	// Timestamps are truncated to microseconds — timestamptz precision and
	// what the hash chain attests (ADR 0029 §2).
	occurred := p.OccurredAt
	if occurred.IsZero() {
		occurred = time.Now()
	}
	occurred = es.TruncateMicros(occurred)
	canonical := p.StreamID.Canonical()

	// Optimistic-concurrency guard and chain head in one read (ADR 0029 §1).
	current, prev, err := w.readHead(ctx, db, p.StreamID)
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
	if err := db.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&recorded); err != nil {
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
		err := db.QueryRow(ctx,
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

	if err := w.applyClaims(ctx, db, canonical, p.Constraints); err != nil {
		return zero, err
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
func (w *Workspace) readHead(ctx context.Context, tx DBTX, sid es.StreamID) (uint64, [es.HashSize]byte, error) {
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
func (w *Workspace) applyClaims(ctx context.Context, tx DBTX, streamID string, ops []es.ConstraintOp) error {
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

// ReadStream implements es.Store. It opens and owns a transaction.
func (w *Workspace) ReadStream(ctx context.Context, sid es.StreamID, fromVersion, toVersion uint64) ([]es.Envelope, error) {
	if err := sid.Validate(); err != nil {
		return nil, err
	}
	tx, err := w.store.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if err := setWorkspace(ctx, tx, w.ws); err != nil {
		return nil, err
	}
	return w.readStream(ctx, tx, sid, fromVersion, toVersion)
}

// ReadStreamTx reads inside a transaction the caller already owns. It never
// commits or rolls back that transaction, and it sets app.workspace_id on db
// (is_local=true) exactly as AppendTx does. db must be an active transaction;
// tenancy.TenantTx satisfies it directly.
func (w *Workspace) ReadStreamTx(ctx context.Context, db DBTX, sid es.StreamID, fromVersion, toVersion uint64) ([]es.Envelope, error) {
	if err := sid.Validate(); err != nil {
		return nil, err
	}
	if err := setWorkspace(ctx, db, w.ws); err != nil {
		return nil, err
	}
	return w.readStream(ctx, db, sid, fromVersion, toVersion)
}

func (w *Workspace) readStream(ctx context.Context, db DBTX, sid es.StreamID, fromVersion, toVersion uint64) ([]es.Envelope, error) {
	q := `SELECT ` + envelopeCols + ` FROM events
	      WHERE workspace_id = $1 AND stream_id = $2 AND version > $3`
	args := []any{w.ws, sid.Canonical(), fromVersion}
	if toVersion > 0 {
		q += ` AND version <= $4`
		args = append(args, toVersion)
	}
	q += ` ORDER BY version ASC`
	return queryDB(ctx, db, q, args...)
}

func (w *Workspace) ReadStreamAsOf(ctx context.Context, sid es.StreamID, asOf time.Time) ([]es.Envelope, error) {
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
func (w *Workspace) ReadAll(ctx context.Context, fromPosition uint64, limit int) ([]es.Envelope, error) {
	if limit <= 0 {
		limit = 100
	}
	q := `SELECT ` + envelopeCols + ` FROM events
	      WHERE workspace_id = $1 AND global_position > $2
	      ORDER BY global_position ASC LIMIT $3`
	return w.query(ctx, q, w.ws, fromPosition, limit)
}

// ReadAllBounded is ReadAll restricted to the given type URLs and bounded by
// a payload-byte budget: the page a live feed or rebuild reads next. types is
// the server-side filter — only rows whose type_url is in types are returned
// (matched in SQL, before any payload is walked) and an empty types set
// returns no events at all. The page holds at most limit events after from,
// in global order, but stops before an event whose payload would push the
// running payload total past maxBytes and reports truncated=true, so the
// caller continues immediately instead of waiting out a poll. The first
// event of a page is always included, even alone over budget: an oversized
// event must be visible to the caller, not forever skipped.
//
// The budget counts the payload bytes this store returns, opaquely (ADR
// 0025): for a sealed workspace these are the stored ciphertext bytes, and a
// plaintext budget belongs to the sealing adapter above the store. maxBytes
// <= 0 disables the budget. Like ReadAll it is deliberately not on es.Store.
func (w *Workspace) ReadAllBounded(ctx context.Context, from uint64, limit, maxBytes int, types []string) ([]es.Envelope, bool, error) {
	if len(types) == 0 {
		return nil, false, nil
	}
	if limit <= 0 {
		limit = 100
	}
	tx, err := w.store.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(ctx)
	if err := setWorkspace(ctx, tx, w.ws); err != nil {
		return nil, false, err
	}
	rows, err := tx.Query(ctx,
		`SELECT `+envelopeCols+` FROM events
	      WHERE workspace_id = $1 AND global_position > $2 AND type_url = ANY($4)
	      ORDER BY global_position ASC LIMIT $3`,
		w.ws, from, limit, types)
	if err != nil {
		return nil, false, err
	}
	var (
		out       []es.Envelope
		total     int
		truncated bool
	)
	for rows.Next() {
		e, err := scanEnvelope(rows)
		if err != nil {
			rows.Close()
			return nil, false, err
		}
		// len(out) == 0 carries the first row whatever its size; after
		// that, stop before a row that would push the total past the
		// budget. The row that stopped the page was read, so truncated is
		// certain — more rows remain for the next call.
		if len(out) > 0 && maxBytes > 0 && total+len(e.Payload) > maxBytes {
			truncated = true
			break
		}
		total += len(e.Payload)
		out = append(out, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	return out, truncated, nil
}

func (w *Workspace) CurrentStreamVersion(ctx context.Context, sid es.StreamID) (uint64, error) {
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

// Head returns the workspace's current log head: the highest
// global_position in the workspace, or 0 when it has no events. It is the
// position a live feed resumes from and the checkpoint a reset rewinds to.
// It is deliberately not on es.Store: it exists for feed plumbing, not for
// the append/read contract.
func (w *Workspace) Head(ctx context.Context) (uint64, error) {
	tx, err := w.store.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if err := setWorkspace(ctx, tx, w.ws); err != nil {
		return 0, err
	}
	var head uint64
	err = tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(global_position), 0) FROM events WHERE workspace_id = $1`,
		w.ws,
	).Scan(&head)
	return head, err
}

// LookupClaim reads the uniqueness index (unique_claims) for the stream holding
// (scope, value). The value is keyed exactly as applyClaims stores it — opaque
// bytes as given (ADR 0025) — so writes and lookups agree. A personal-data
// lookup must pass the same caller-computed MAC the write used.
func (w *Workspace) LookupClaim(ctx context.Context, scope, value string) (string, bool, error) {
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
func (w *Workspace) query(ctx context.Context, q string, args ...any) ([]es.Envelope, error) {
	tx, err := w.store.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if err := setWorkspace(ctx, tx, w.ws); err != nil {
		return nil, err
	}
	return queryDB(ctx, tx, q, args...)
}

// queryDB runs a workspace-scoped read through db. The caller has set the RLS
// variable and owns the transaction (or passes a transaction-shaped handle).
func queryDB(ctx context.Context, db DBTX, q string, args ...any) ([]es.Envelope, error) {
	rows, err := db.Query(ctx, q, args...)
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
