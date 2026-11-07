package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/feed"
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

	// Horizon-fence state (spec §5.1): boundL is the sequence value L a
	// round recorded, fenceR the transaction id R the reader drew
	// immediately after reading it. fenceR == 0 means no fence is stored.
	// Two integers on this per-scope instance — the feed opens one
	// Workspace per stream — guarded because Status may touch the same
	// instance concurrently.
	//
	// Why bounding pages this way is lossless (spec §5.1.3):
	//
	//   - P1 — the append draws its xid before its position. append issues
	//     one bare read-only statement, SELECT pg_current_xact_id(), as the
	//     transaction's first statement, before readHead and before any
	//     nextval position draw. Hence every event row's insert-xid was
	//     assigned before its position was drawn. (Folding the xid draw
	//     into readHead's SELECT was tried and falsified on empty streams
	//     by the Task 1b spike: readHead returns no row on an empty
	//     stream, so the target list never evaluates and no xid is
	//     assigned — every stream's first append would lose P1. The bare
	//     statement assigns unconditionally; the xid is stable across the
	//     whole append transaction.)
	//   - P2 — positions are monotonic. Any row at a position <= L drew
	//     that position before the reader read L, and therefore (by P1)
	//     has an insert-xid assigned before R was drawn, i.e. an xid < R.
	//
	// While any xid < R is still in flight, pg_snapshot_xmin stays below
	// R — the Task 1b spike verified both halves: an in-flight xid below
	// the commit horizon appears in the snapshot's in-flight list and
	// lowers xmin, and an in-flight xid at or above the horizon pins the
	// horizon itself, which is xmin's floor when nothing older runs. The
	// reader's own R-abort is what raises the horizon past the appenders'
	// xids and makes them snapshot-visible in the first place. So xmin >= R
	// implies every xid < R is committed or aborted. Combined with P2:
	// when the fence clears, every row at a position <= L is either
	// already visible or was aborted — nothing inside the delivered range
	// can appear later. Aborted appends leave permanent holes above the
	// cursor, which the type filter already treats as ordinary
	// cross-workspace gaps.
	mu     sync.Mutex
	boundL uint64
	fenceR uint64
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

	// Payload cap (production review): reject the whole batch before any
	// statement runs — before the xid is drawn, before any insert — so a
	// caller-owned transaction (AppendTx) is refused too and nothing
	// reaches the log. A single oversized event otherwise wedges every
	// feed subscription consuming its type. The error carries stream,
	// version, type URL and size only — never the payload bytes.
	for i, ev := range p.Events {
		if w.store.maxPayloadBytes > 0 && len(ev.Payload) > w.store.maxPayloadBytes {
			version := p.ExpectedVersion + uint64(i) + 1
			return zero, fmt.Errorf("%w: stream %s v%d, type %s, payload %d bytes exceeds cap %d",
				es.ErrPayloadTooLarge, p.StreamID, version, ev.TypeURL, len(ev.Payload), w.store.maxPayloadBytes)
		}
	}

	// P1 (spec §5.1.3): assign this transaction's xid before any position
	// is drawn. One bare read-only statement, before readHead: the xid is
	// assigned here unconditionally — including on a stream's first
	// append — and is stable for the rest of the transaction, so every
	// event row inserted below carries an insert-xid drawn strictly
	// before its nextval() position. (Never fold this into readHead: on an
	// empty stream readHead returns no row, its target list never
	// evaluates, and no xid is assigned — the Task 1b spike falsified the
	// fold and verified this shape. One extra round trip per append, the
	// accepted price of commit-safe reads.)
	var xid uint64
	if err := db.QueryRow(ctx, `SELECT pg_current_xact_id()`).Scan(&xid); err != nil {
		return zero, fmt.Errorf("assign append xid: %w", err)
	}

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
//
// A read from the beginning of a stream that was never written —
// fromVersion 0 and no rows for the stream at all, whatever toVersion is —
// returns an error wrapping es.ErrStreamNotFound (with the canonical
// stream id in the message) instead of a silent empty slice. An empty
// range is not "not found": reads with fromVersion > 0, and reads from 0
// of a stream that exists, return (nil, nil).
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
	envs, err := w.readStream(ctx, tx, sid, fromVersion, toVersion)
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
			exists, err := w.streamExists(ctx, tx, sid)
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
// written to the stream. Runs with the workspace RLS variable set, on the
// same transaction as the read it decides for.
func (w *Workspace) streamExists(ctx context.Context, db DBTX, sid es.StreamID) (bool, error) {
	var one int
	err := db.QueryRow(ctx,
		`SELECT 1 FROM events WHERE workspace_id = $1 AND stream_id = $2 LIMIT 1`,
		w.ws, sid.Canonical(),
	).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// ReadStreamTx reads inside a transaction the caller already owns. It never
// commits or rolls back that transaction, and it sets app.workspace_id on db
// (is_local=true) exactly as AppendTx does. db must be an active transaction;
// tenancy.TenantTx satisfies it directly.
//
// Unlike ReadStream it does not report ErrStreamNotFound: a read of a
// stream with no events returns (nil, nil), so logic inside a
// caller-owned transaction always deals in plain ranges, never the
// not-found sentinel.
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

// The fence statements (spec §5.1.1) live at Store level: the fence is
// transaction-global — it waits out every transaction that existed when R
// was drawn, whatever workspace appended — so the anchor path
// (settledBound, anchor.go), which spans workspaces by construction,
// shares them with the feed through the Workspace delegations below.

// readLastPosition is fence statement 1 (spec §5.1.1): the position
// sequence's last_value, read outside any transaction. It reflects every
// position drawn so far, committed or not — conservative and safe, because
// delivery waits on the fence, not on L. A fresh sequence reports
// last_value=1 before any draw; L can exceed the highest drawn position.
func (s *Store) readLastPosition(ctx context.Context) (uint64, error) {
	var last uint64
	err := s.pool.QueryRow(ctx,
		`SELECT last_value FROM events_global_position_seq`,
	).Scan(&last)
	if err != nil {
		return 0, fmt.Errorf("read last position: %w", err)
	}
	return last, nil
}

// drawFence is fence statement 2 (spec §5.1.1): draw the reader's own
// transaction id in a short transaction, then roll back. The abort settles R
// immediately — the commit horizon advances to R itself, which is what
// makes in-flight appenders snapshot-visible to the next statement (Task 1b
// verified; without it a lone in-flight append looks idle to every
// snapshot).
func (s *Store) drawFence(ctx context.Context) (uint64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin fence: %w", err)
	}
	defer tx.Rollback(ctx)
	var r uint64
	if err := tx.QueryRow(ctx, `SELECT pg_current_xact_id()`).Scan(&r); err != nil {
		return 0, fmt.Errorf("draw fence xid: %w", err)
	}
	return r, nil
}

// fenceCleared is fence statement 3 (spec §5.1.1): true once no transaction
// that already existed when R was drawn is still in flight, observed as
// the snapshot horizon having reached R.
func (s *Store) fenceCleared(ctx context.Context, r uint64) (bool, error) {
	var cleared bool
	err := s.pool.QueryRow(ctx,
		`SELECT pg_snapshot_xmin(pg_current_snapshot()) >= $1`, r,
	).Scan(&cleared)
	if err != nil {
		return false, fmt.Errorf("check fence: %w", err)
	}
	return cleared, nil
}

// The Workspace fence round delegates to the Store-level statements above:
// the workspace scope adds only the (boundL, fenceR) state below.

func (w *Workspace) readLastPosition(ctx context.Context) (uint64, error) {
	return w.store.readLastPosition(ctx)
}

func (w *Workspace) drawFence(ctx context.Context) (uint64, error) {
	return w.store.drawFence(ctx)
}

func (w *Workspace) fenceCleared(ctx context.Context, r uint64) (bool, error) {
	return w.store.fenceCleared(ctx, r)
}

// fenceBound runs one horizon-fence round (spec §5.1.1) and returns the
// position bound a page may deliver up to. Zero means deliver nothing now:
// either an established fence is still blocked (the state stays stored and
// the caller's wait cycle retries), or the log is idle at from (no fence
// drawn — idle streams burn no transaction ids). A cleared fence drops the
// state; the next delivered page re-establishes a fresh fence.
func (w *Workspace) fenceBound(ctx context.Context, from uint64) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.fenceR != 0 {
		cleared, err := w.fenceCleared(ctx, w.fenceR)
		if err != nil {
			return 0, err
		}
		if !cleared {
			return 0, nil
		}
		bound := w.boundL
		w.boundL, w.fenceR = 0, 0
		return bound, nil
	}

	last, err := w.readLastPosition(ctx)
	if err != nil {
		return 0, err
	}
	if last <= from {
		return 0, nil
	}
	r, err := w.drawFence(ctx)
	if err != nil {
		return 0, err
	}
	cleared, err := w.fenceCleared(ctx, r)
	if err != nil {
		return 0, err
	}
	if cleared {
		return last, nil
	}
	w.boundL, w.fenceR = last, r
	return 0, nil
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
// The page is fetched in two steps so a payload over the store's cap
// (WithMaxPayloadBytes) is never materialized (production review): step 1
// selects positions, type URLs and octet_length(payload) — sizes only,
// never the bytes — under the filter and applies the count/budget/cap
// selection; step 2 fetches full rows for the chosen positions alone. A
// first row over the cap is a poison event: the page refuses to carry it
// and returns *feed.EventTooLargeError naming position, type URL, size and
// limit — never the payload. A later row over the cap stops the page
// before it with truncated=true, so the next page makes it the first row
// and surfaces the error.
//
// The page is also bounded by the horizon fence (spec §5.1): positions are
// only delivered up to the L recorded at the start of the round, and only
// once every transaction that existed when the round drew its R has
// settled — the horizon check is transaction-global, not append-specific.
// While such a transaction is in flight the page is empty
// (truncated=false) and the fence stays stored on this Workspace; the
// feed's wait cycle retries. A delivered page therefore never has a live
// hole inside it — nothing in the delivered range can appear later — and
// an aborted append's permanent hole is passed like any other gap.
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
	bound, err := w.fenceBound(ctx, from)
	if err != nil {
		return nil, false, err
	}
	if bound <= from {
		return nil, false, nil
	}
	tx, err := w.store.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(ctx)
	if err := setWorkspace(ctx, tx, w.ws); err != nil {
		return nil, false, err
	}
	// Step 1: positions, type URLs and payload sizes under the filter and
	// the fence bound — never the payload bytes. octet_length measures in
	// the database, so an over-cap row costs one integer, not its payload.
	rows, err := tx.Query(ctx,
		`SELECT global_position, type_url, octet_length(payload) FROM events
	      WHERE workspace_id = $1 AND global_position > $2 AND type_url = ANY($4)
	          AND global_position <= $5
	      ORDER BY global_position ASC LIMIT $3`,
		w.ws, from, limit, types, bound)
	if err != nil {
		return nil, false, err
	}
	var (
		positions []uint64
		total     int
		truncated bool
	)
	for rows.Next() {
		var (
			pos     uint64
			typeURL string
			size    int
		)
		if err := rows.Scan(&pos, &typeURL, &size); err != nil {
			rows.Close()
			return nil, false, err
		}
		if len(positions) == 0 && w.store.maxPayloadBytes > 0 && size > w.store.maxPayloadBytes {
			// The first row of the page is over the store's cap: a poison
			// event. Refuse to materialize it; the typed error names it
			// without the payload bytes ever leaving the database.
			rows.Close()
			return nil, false, &feed.EventTooLargeError{
				Position: pos,
				TypeURL:  typeURL,
				Size:     size,
				Limit:    w.store.maxPayloadBytes,
			}
		}
		// A later row over the cap stops the page before it — the next
		// page makes it the first row and surfaces the error — and so does
		// a row that would push the total past the budget. The stopping
		// row was seen, so truncated is certain: more rows remain for the
		// next call.
		if len(positions) > 0 &&
			((w.store.maxPayloadBytes > 0 && size > w.store.maxPayloadBytes) ||
				(maxBytes > 0 && total+size > maxBytes)) {
			truncated = true
			break
		}
		total += size
		positions = append(positions, pos)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if len(positions) == 0 {
		return nil, false, nil
	}
	// Step 2: fetch full rows for the chosen positions only, in the same
	// transaction and therefore the same snapshot — the page cannot change
	// under the fetch, and no payload the selection did not clear is ever
	// selected. global_position is bigint, so positions always fit int64
	// (pgx sends []uint64 as numeric[], which would force a cast).
	positions64 := make([]int64, len(positions))
	for i, p := range positions {
		positions64[i] = int64(p)
	}
	out, err := queryDB(ctx, tx,
		`SELECT `+envelopeCols+` FROM events
		  WHERE workspace_id = $1 AND global_position = ANY($2)
		  ORDER BY global_position ASC`,
		w.ws, positions64)
	if err != nil {
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

// Head returns the workspace's settled log head: the highest
// global_position whose append is known to have settled — never a position
// at or above one still in flight. It is the position a live feed resumes
// from and the checkpoint a reset rewinds to: a value a caller may ack to
// without skipping an event that commits later. It is deliberately not on
// es.Store: it exists for feed plumbing, not for the append/read contract.
//
// Head is eventually consistent. It shares this Workspace's horizon fence
// with ReadAllBounded (spec §5.1) — the same (boundL, fenceR) round — so
// while that fence is blocked by an in-flight transaction, Head reports a
// value below the raw max: 0 until a fence clears, then whatever has
// settled up to the clearing round's L. It converges to the full settled
// max once the blocking transaction commits or aborts. Callers that need
// the immediate committed maximum — for stall detection — use RawHead.
func (w *Workspace) Head(ctx context.Context) (uint64, error) {
	// One fence round (spec §5.1.1), from 0: Head reads the whole log, so
	// any drawn position is new and the idle shortcut never fires — every
	// call either checks the stored fence or draws a fresh one. A zero
	// bound means no settled range yet — a transaction is in flight that
	// blocks the fence — and Head reports 0 rather than a position it
	// would have to take back.
	bound, err := w.fenceBound(ctx, 0)
	if err != nil {
		return 0, err
	}
	if bound == 0 {
		return 0, nil
	}
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
		`SELECT COALESCE(MAX(global_position), 0) FROM events
		  WHERE workspace_id = $1 AND global_position <= $2`,
		w.ws, bound,
	).Scan(&head)
	return head, err
}

// MatchingHead returns the highest settled position among events of the
// given types — the subscription's own head, what Status reports and
// measures lag against. It is fence-bounded exactly like Head (spec
// §5.1): it shares this Workspace's (boundL, fenceR) round, so while the
// fence is blocked it reports 0, and a probe while blocked reuses the
// stored round — one horizon check, no fresh fence, no transaction id
// burned. An empty types set matches nothing: 0. Deliberately not on
// es.Store.
func (w *Workspace) MatchingHead(ctx context.Context, types []string) (uint64, error) {
	if len(types) == 0 {
		return 0, nil
	}
	// One fence round from 0, like Head: MatchingHead reads the whole
	// log's matching maximum, so any drawn position is new and the idle
	// shortcut never fires. A zero bound means the fence is blocked —
	// report 0 rather than a position above the unsettled horizon.
	bound, err := w.fenceBound(ctx, 0)
	if err != nil {
		return 0, err
	}
	if bound == 0 {
		return 0, nil
	}
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
		`SELECT COALESCE(MAX(global_position), 0) FROM events
		  WHERE workspace_id = $1 AND global_position <= $2 AND type_url = ANY($3)`,
		w.ws, bound, types,
	).Scan(&head)
	return head, err
}

// RawHead returns the workspace's raw log head: the highest
// global_position among the workspace's committed events, read immediately
// with no fence and no bounding. It can jump past positions whose appends
// are still in flight — the raw max is not a settle point — so it is not a
// resume position: a feed that acked to RawHead would skip an in-flight
// event when it commits. It exists for stall detection only (spec §5.8):
// RawHead above the feed's position is committed backlog the settled reads
// have not delivered, the signal to report OnStall; probe it rate-limited.
// Deliberately not on es.Store.
func (w *Workspace) RawHead(ctx context.Context) (uint64, error) {
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
