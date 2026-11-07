package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/laenenai/es-lite/es"
)

var _ es.Anchorer = (*Store)(nil)

// ChainedSince returns when migration 0002 (the hash chain) was applied, for
// es.VerifyOptions.ChainedSince. The zero time means it has not run.
func (s *Store) ChainedSince(ctx context.Context) (time.Time, error) {
	var applied time.Time
	err := s.pool.QueryRow(ctx, `SELECT applied_at FROM schema_migrations WHERE version = 2`).Scan(&applied)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	return applied.UTC(), nil
}

// bypassTx opens a transaction with RLS bypassed — the admin/relay path, as
// Drain uses (ADR 0004). Anchors span every workspace by construction.
// The bypass is role-gated (migration 0005): like Drain, the session's
// user must be an es_relay member — verified here, once per Store —
// because the events policies hide every row from a non-member without
// an error, and this path must not inherit that silence: Anchor would
// otherwise record a root over zero heads as a real anchor, CheckAnchor
// would report a spurious mismatch, and ListStreams would return
// nothing.
func (s *Store) bypassTx(ctx context.Context) (pgx.Tx, error) {
	if err := s.checkRelayRole(ctx); err != nil {
		return nil, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.bypass_rls', 'on', true)`); err != nil {
		tx.Rollback(ctx)
		return nil, fmt.Errorf("set bypass: %w", err)
	}
	return tx, nil
}

// anchorFencePoll is the cadence settledBound rechecks the fence at while
// an in-flight transaction blocks it.
const anchorFencePoll = 100 * time.Millisecond

// settledBound returns a global_position bound that is permanently
// settled: every event row at or below it that will ever exist has
// committed (or its append aborted, leaving a permanent hole), so heads
// computed under it can never change again.
//
// It is the anchor path's horizon-fence round (spec §5.1.1), over the same
// fence statements the feed uses. upto == 0 means the whole log and takes
// the sequence's last_value — the highest position drawn, committed or
// not; a caller's bound above last_value is clamped to it, since the fence
// only certifies drawn positions. The round then draws its own xid R in a
// rolled-back transaction and polls fenceCleared until the snapshot
// horizon reaches R: at that point every transaction that existed when R
// was drawn has settled, so — by P1/P2 (spec §5.1.3; see the Workspace
// fence state's doc) — every position drawn at or below last_value is
// final, and nothing can ever land at or below the bound again (later
// appends draw higher positions). The wait holds no transaction open and
// adds no lock; ctx ending aborts it.
//
// Without this round, Anchor's two READ COMMITTED statements tear: the
// committed max can sit above an in-flight lower position (Postgres
// assigns global_position at INSERT, not at COMMIT), the heads statement
// then misses the in-flight row, and the recorded root is disputed by a
// later CheckAnchor with a false ErrAnchorMismatch once the row commits
// (F1).
func (s *Store) settledBound(ctx context.Context, upto uint64) (uint64, error) {
	last, err := s.readLastPosition(ctx)
	if err != nil {
		return 0, err
	}
	// 0 = the log head; a bound beyond last_value asks for positions the
	// fence cannot certify, so it clamps down to the drawn horizon.
	if upto == 0 || upto > last {
		upto = last
	}
	r, err := s.drawFence(ctx)
	if err != nil {
		return 0, err
	}
	for {
		cleared, err := s.fenceCleared(ctx, r)
		if err != nil {
			// A ctx that ends while the poll's round trip is in
			// flight must surface the mandated wording, not the
			// poll's transport error: the operator promise is the
			// fence's own phrase wrapping the ctx's cause.
			if e := ctx.Err(); e != nil {
				return 0, fmt.Errorf("postgres: anchor fence still blocked: %w", e)
			}
			return 0, err
		}
		if cleared {
			return upto, nil
		}
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("postgres: anchor fence still blocked: %w", ctx.Err())
		case <-time.After(anchorFencePoll):
		}
	}
}

// Anchor implements es.Anchorer: it computes the root over every stream's
// head at or below upto (0 = the log head) across all workspaces, records
// the row and returns it. It also computes the order root over the
// log_order rows at or below the same bound (design D1) and returns it
// in the anchor's three order fields — the recorded row keeps the five
// legacy columns; the order root travels with the exported anchor, the
// off-box witness. Runs under app.bypass_rls.
//
// The bound is fenced: settledBound resolves upto to a permanently
// settled position before any head is read, so a concurrent append below
// the head cannot tear the root — the anchor is recorded either after
// that append settled (its row included) or below it (its position
// excluded), and CheckAnchor agrees forever after. The order rows join
// the same settled section, so the order root is exactly as permanent.
func (s *Store) Anchor(ctx context.Context, upto uint64) (es.Anchor, error) {
	// The role gate precedes the fence (bypassTx re-verifies for free,
	// once per Store): a non-member session must fail fast with the typed
	// error, never wait out a fence round it is not allowed to use.
	if err := s.checkRelayRole(ctx); err != nil {
		return es.Anchor{}, err
	}
	upto, err := s.settledBound(ctx, upto)
	if err != nil {
		return es.Anchor{}, err
	}
	tx, err := s.bypassTx(ctx)
	if err != nil {
		return es.Anchor{}, err
	}
	defer tx.Rollback(ctx)

	heads, err := heads(ctx, tx, upto)
	if err != nil {
		return es.Anchor{}, err
	}
	root := es.AnchorRoot(heads)
	// The order rows join the same settled, fenced section: a row and
	// its events commit in one transaction, so every log_order row at
	// or below the bound is as permanently final as the heads are —
	// nothing inside the bound can appear later (spec §5.1.3).
	rows, err := orderRows(ctx, tx, upto)
	if err != nil {
		return es.Anchor{}, err
	}
	orderRoot := es.OrderRoot(rows)
	a := es.Anchor{
		ID:                 newV7().String(),
		CreatedAt:          es.TruncateMicros(time.Now()),
		UptoGlobalPosition: upto,
		StreamCount:        uint64(len(heads)),
		RootHash:           root[:],
		// A new-style anchor always carries an order root — over an
		// empty row set when the bound covers no batch (a real hash, per
		// the empty-set contract; nil is exclusively the legacy marker
		// for anchors recorded before log-order attestation).
		OrderRootHash: orderRoot[:],
		OrderCount:    uint64(len(rows)),
	}
	// rows is from_position-ascending: the first row's From is the first
	// position the root attests; 0 (the zero value) when none are.
	if len(rows) > 0 {
		a.OrderFromPosition = rows[0].From
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO anchors (anchor_id, created_at, upto_global_position, stream_count, root_hash) VALUES ($1,$2,$3,$4,$5)`,
		a.ID, a.CreatedAt, a.UptoGlobalPosition, a.StreamCount, a.RootHash,
	); err != nil {
		return es.Anchor{}, fmt.Errorf("insert anchor: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return es.Anchor{}, fmt.Errorf("commit: %w", err)
	}
	return a, nil
}

// ListAnchors implements es.Anchorer.
func (s *Store) ListAnchors(ctx context.Context) ([]es.Anchor, error) {
	tx, err := s.bypassTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx,
		`SELECT anchor_id, created_at, upto_global_position, stream_count, root_hash FROM anchors ORDER BY upto_global_position, created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []es.Anchor
	for rows.Next() {
		var a es.Anchor
		if err := rows.Scan(&a.ID, &a.CreatedAt, &a.UptoGlobalPosition, &a.StreamCount, &a.RootHash); err != nil {
			return nil, err
		}
		a.CreatedAt = a.CreatedAt.UTC()
		out = append(out, a)
	}
	return out, rows.Err()
}

// CheckAnchor implements es.Anchorer: recomputes the root at the anchor's
// position and compares. The recompute is fenced exactly like Anchor's —
// settledBound resolves the bound first — so the verdict is taken over
// permanently settled heads: it cannot miss an append still in flight
// below the anchor's bound, and an agreement holds. A new-style anchor
// (non-nil OrderRootHash) has its three order fields compared over the
// same settled section; a legacy anchor (nil) skips them and verifies
// heads-only.
func (s *Store) CheckAnchor(ctx context.Context, a es.Anchor) error {
	// The role gate precedes the fence, as in Anchor: a non-member session
	// fails fast with the typed error, never a spurious verdict.
	if err := s.checkRelayRole(ctx); err != nil {
		return err
	}
	upto, err := s.settledBound(ctx, a.UptoGlobalPosition)
	if err != nil {
		return err
	}
	tx, err := s.bypassTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	hs, err := heads(ctx, tx, upto)
	if err != nil {
		return err
	}
	root := es.AnchorRoot(hs)
	if uint64(len(hs)) != a.StreamCount || !bytes.Equal(root[:], a.RootHash) {
		return fmt.Errorf("%w: anchor %s at global_position %d (streams %d, recomputed %d)",
			es.ErrAnchorMismatch, a.ID, a.UptoGlobalPosition, a.StreamCount, len(hs))
	}
	// The order fields compare only on a new-style anchor: nil
	// OrderRootHash is exclusively the legacy marker for an anchor
	// recorded before log-order attestation, and such an anchor
	// verifies heads-only.
	if a.OrderRootHash == nil {
		return nil
	}
	rows, err := orderRows(ctx, tx, upto)
	if err != nil {
		return err
	}
	orderRoot := es.OrderRoot(rows)
	var from uint64
	if len(rows) > 0 {
		from = rows[0].From
	}
	if !bytes.Equal(orderRoot[:], a.OrderRootHash) || from != a.OrderFromPosition || uint64(len(rows)) != a.OrderCount {
		return fmt.Errorf("%w: anchor %s at global_position %d (order rows %d, recomputed %d; order from %d, recomputed %d; order root %x, recomputed %x)",
			es.ErrAnchorMismatch, a.ID, a.UptoGlobalPosition, a.OrderCount, len(rows), a.OrderFromPosition, from, a.OrderRootHash, orderRoot[:])
	}
	return nil
}

// ListStreams implements es.Anchorer: every (workspace, stream) in the
// store, under app.bypass_rls.
func (s *Store) ListStreams(ctx context.Context) ([]es.StreamRef, error) {
	tx, err := s.bypassTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT DISTINCT workspace_id, stream_id FROM events ORDER BY workspace_id, stream_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []es.StreamRef
	for rows.Next() {
		var ws, canonical string
		if err := rows.Scan(&ws, &canonical); err != nil {
			return nil, err
		}
		sid, err := es.ParseCanonical(canonical)
		if err != nil {
			return nil, err
		}
		out = append(out, es.StreamRef{Workspace: ws, StreamID: sid})
	}
	return out, rows.Err()
}

// OrderRows implements es.Anchorer: the log_order rows with
// to_position <= upto, under app.bypass_rls (the role-gated admin path,
// as for every cross-workspace read). upto == 0 reads the whole log —
// which is exactly "the current log head": every committed row's
// to_position was drawn from the position sequence, so no committed row
// can sit above it. The read takes no fence round because it records
// nothing; the paths that freeze rows into a root (Anchor, CheckAnchor,
// VerifyOrder) read them inside their own settled, fenced section
// through the same helper.
func (s *Store) OrderRows(ctx context.Context, upto uint64) ([]es.OrderRow, error) {
	tx, err := s.bypassTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	return orderRows(ctx, tx, upto)
}

// logOrderRow is one log_order row with the scope columns es.OrderRow
// drops — workspace and canonical stream — which the deep verify needs
// to group events to their batch.
type logOrderRow struct {
	es.OrderRow
	Workspace string
	Stream    string // canonical "type:id", as events stores it

	// entries are the events VerifyOrder found in the row's range, in
	// the position order the pages delivered them (ascending).
	entries []es.OrderEntry
}

// readLogOrderRows reads the log_order rows with to_position <= upto
// (upto == 0: no clamp, the whole log) under tx, in ascending
// from_position order.
func readLogOrderRows(ctx context.Context, tx pgx.Tx, upto uint64) ([]logOrderRow, error) {
	q := `SELECT from_position, to_position, event_count, batch_hash, workspace_id, stream_id FROM log_order`
	args := []any{}
	if upto > 0 {
		q += ` WHERE to_position <= $1`
		args = append(args, upto)
	}
	q += ` ORDER BY from_position`
	rows, err := tx.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("read log_order: %w", err)
	}
	defer rows.Close()
	var out []logOrderRow
	for rows.Next() {
		var r logOrderRow
		if err := rows.Scan(&r.From, &r.To, &r.Count, &r.BatchHash, &r.Workspace, &r.Stream); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// orderRows adapts readLogOrderRows to the es.OrderRow shape the
// anchor paths aggregate.
func orderRows(ctx context.Context, tx pgx.Tx, upto uint64) ([]es.OrderRow, error) {
	full, err := readLogOrderRows(ctx, tx, upto)
	if err != nil {
		return nil, err
	}
	out := make([]es.OrderRow, len(full))
	for i := range full {
		out[i] = full[i].OrderRow
	}
	return out, nil
}

// OrderVerifyReport is the verdict of VerifyOrder, the deep log-order
// verification (design D1, F11). The type lives in the core —
// es.OrderVerifyReport — so both backends report the same shape to
// `es-verify order`; this alias keeps the postgres name for existing
// references.
type OrderVerifyReport = es.OrderVerifyReport

// OrderRowMismatch is the first log_order row whose recomputed
// es.BatchOrderHash differs from the stored batch_hash; aliased from
// es alongside the report type, whose Mismatch field carries it.
type OrderRowMismatch = es.OrderRowMismatch

// orderVerifyPageSize is the event page size VerifyOrder reads positions
// at: large enough that verifying a big log is a few round trips per
// thousand events, small enough that no page's payloads stress memory
// (the cost class is VerifyStream's, which also reads full envelopes).
const orderVerifyPageSize = 1000

// readPositions pages events by global position under tx (the caller
// owns the RLS bypass): the envelopes with global_position > from and
// <= to, ascending, at most limit of them. It reuses envelopeCols — the
// column shape every envelope read shares — so the deep verify walks
// exactly what the store returns everywhere else. Positions are bigint:
// the bounds cross the wire as int64 (pgx sends uint64 as numeric, the
// ReadAllBounded comment's detour).
func readPositions(ctx context.Context, tx pgx.Tx, from, to uint64, limit int) ([]es.Envelope, error) {
	return queryDB(ctx, tx,
		`SELECT `+envelopeCols+` FROM events
		  WHERE global_position > $1 AND global_position <= $2
		  ORDER BY global_position ASC LIMIT $3`,
		int64(from), int64(to), limit)
}

// orderScope keys the log_order rows of one (workspace, stream) — the
// scope a single append writes. Same-stream batches never share
// positions: a later same-stream append draws its positions only after
// the earlier one committed (or it loses the version race and writes
// nothing), so each scope's rows are disjoint and from_position
// ascending — the invariant coveringRow's binary search relies on.
type orderScope struct {
	workspace string
	stream    string
}

// coveringRow returns the row of rs whose [From, To] range contains
// pos, or nil. rs is from_position ascending; the rightmost row
// starting at or below pos is the only candidate, and if its range does
// not reach pos, no row does. (A tampered log can break the
// disjointness — overlapping rows, positions moved across scopes — and
// then this attribution is merely deterministic: the hash comparison,
// not the attribution, is what reports the tamper.)
func coveringRow(rs []*logOrderRow, pos uint64) *logOrderRow {
	i := sort.Search(len(rs), func(i int) bool { return rs[i].From > pos })
	if i == 0 {
		return nil
	}
	r := rs[i-1]
	if r.To >= pos {
		return r
	}
	return nil
}

// VerifyOrder deep-verifies the log-order attestation (design D1,
// F11): for every log_order row at or below a settled bound it
// recomputes the row's es.BatchOrderHash from the events themselves —
// paged by position under the RLS bypass, grouped to each row by its
// (workspace, stream) scope and its [from_position, to_position]
// range — and reports the first row whose recomputed hash disagrees
// with the stored batch_hash. This is the check that catches a
// repositioned, inserted or deleted event inside an attested batch,
// which the per-stream chains and the heads root all survive; the
// exported anchor's OrderRootHash is the off-box witness that catches
// a wholesale rewrite (ADR 0029's chain/anchor trust split, applied to
// order).
//
// It is an admin path, gated like the anchor path: the role check
// precedes the fence, and the bound comes from the same settledBound
// round — every row and every event at or below it is permanently
// settled, so the verdict cannot be torn by an append committing
// mid-verification: a batch's row and its events commit in one
// transaction, and nothing at or below the bound can appear later.
//
// Events appended before migration 0006 have no log_order row; they are
// unattested and skipped (the order analog of the chain's ChainedFrom).
func (s *Store) VerifyOrder(ctx context.Context, upto uint64) (*OrderVerifyReport, error) {
	// The role gate precedes the fence, as in Anchor: a non-member
	// session fails fast with the typed error, never a verdict it was
	// not allowed to compute.
	if err := s.checkRelayRole(ctx); err != nil {
		return nil, err
	}
	bound, err := s.settledBound(ctx, upto)
	if err != nil {
		return nil, err
	}
	tx, err := s.bypassTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := readLogOrderRows(ctx, tx, bound)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return &OrderVerifyReport{OK: true}, nil
	}

	// Index the rows by scope, keeping the global from_position order
	// per scope; rows are referenced in place so the entries each page
	// adds land on the row being verified.
	byScope := make(map[orderScope][]*logOrderRow)
	for i := range rows {
		r := &rows[i]
		k := orderScope{r.Workspace, r.Stream}
		byScope[k] = append(byScope[k], r)
	}

	// Page the settled log from the first covered position — nothing
	// below it can belong to a row — and group each event to its row.
	// Pages arrive ascending by position, so every row's entries do
	// too: the order BatchOrderHash attests.
	cursor := rows[0].From - 1
	for {
		page, err := readPositions(ctx, tx, cursor, bound, orderVerifyPageSize)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			break
		}
		for i := range page {
			e := &page[i]
			cursor = e.GlobalPosition
			if r := coveringRow(byScope[orderScope{e.Workspace, e.StreamID.Canonical()}], e.GlobalPosition); r != nil {
				r.entries = append(r.entries, es.OrderEntry{Position: e.GlobalPosition, Hash: e.Hash})
			}
			// An event no row covers is a pre-0006 event (unattested)
			// or a tampered position: the row it left behind is what
			// reports the mismatch.
		}
		if len(page) < orderVerifyPageSize {
			break
		}
	}

	// Compare every row in from_position order; the first disagreement
	// is the report's mismatch.
	rep := &OrderVerifyReport{OK: true, RowsChecked: uint64(len(rows))}
	for i := range rows {
		recomputed := es.BatchOrderHash(rows[i].entries)
		if !bytes.Equal(recomputed[:], rows[i].BatchHash) {
			rep.OK = false
			rep.Mismatch = &OrderRowMismatch{
				From:              rows[i].From,
				ExpectedBatchHash: append([]byte(nil), rows[i].BatchHash...),
				ActualBatchHash:   append([]byte(nil), recomputed[:]...),
			}
			break
		}
	}
	return rep, nil
}

// heads returns each (workspace, stream)'s highest version at or below upto
// with its hash.
func heads(ctx context.Context, tx pgx.Tx, upto uint64) ([]es.StreamHead, error) {
	rows, err := tx.Query(ctx,
		`SELECT e.workspace_id, e.stream_id, e.version, e.hash
		   FROM events e
		   JOIN (SELECT workspace_id, stream_id, MAX(version) AS v
		           FROM events WHERE global_position <= $1
		          GROUP BY workspace_id, stream_id) m
		     ON m.workspace_id = e.workspace_id AND m.stream_id = e.stream_id AND m.v = e.version
		  ORDER BY e.workspace_id, e.stream_id`, upto)
	if err != nil {
		return nil, fmt.Errorf("read stream heads: %w", err)
	}
	defer rows.Close()
	var out []es.StreamHead
	for rows.Next() {
		var (
			h         es.StreamHead
			canonical string
		)
		if err := rows.Scan(&h.Workspace, &canonical, &h.Version, &h.Hash); err != nil {
			return nil, err
		}
		if h.StreamID, err = es.ParseCanonical(canonical); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}
