package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/laenenai/es-lite/es"
)

var _ es.Anchorer = (*Store)(nil)

// ChainedSince returns when migration 0002 (the hash chain) was applied, for
// es.VerifyOptions.ChainedSince: any unhashed row recorded after it is a
// break. The zero time means the migration has not run.
func (s *Store) ChainedSince(ctx context.Context) (time.Time, error) {
	var applied string
	err := s.db.QueryRowContext(ctx, `SELECT applied_at FROM schema_migrations WHERE version = 2`).Scan(&applied)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	// schema_migrations.applied_at is strftime('%Y-%m-%dT%H:%M:%fZ') — ms
	// precision, not the fixed-width ns layout events use.
	t, err := time.Parse("2006-01-02T15:04:05.000Z", applied)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse applied_at %q: %w", applied, err)
	}
	return t, nil
}

// Anchor implements es.Anchorer: it computes the root over every stream's
// head at or below upto (0 = the log head), records the row and returns it.
// It also computes the order root over the log_order rows at or below the
// same bound (design D1) and returns it in the anchor's three order
// fields — the recorded row keeps the five legacy columns; the order root
// travels with the exported anchor, the off-box witness.
//
// The bound needs no fence round (adjudication §2.1): the store's single
// connection serializes every append, so the heads and order rows a
// committed bound reads are final — nothing at or below the bound can
// change after the read.
func (s *Store) Anchor(ctx context.Context, upto uint64) (es.Anchor, error) {
	if upto == 0 {
		if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(global_position), 0) FROM events`).Scan(&upto); err != nil {
			return es.Anchor{}, fmt.Errorf("read log head: %w", err)
		}
	}
	heads, err := s.heads(ctx, upto)
	if err != nil {
		return es.Anchor{}, err
	}
	root := es.AnchorRoot(heads)
	// The order rows join the same bounded section: a row and its events
	// commit in one transaction on the single-writer connection, so every
	// log_order row at or below the bound is as final as the heads are.
	rows, err := s.orderRows(ctx, upto)
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
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO anchors (anchor_id, created_at, upto_global_position, stream_count, root_hash) VALUES (?,?,?,?,?)`,
		a.ID, formatTS(a.CreatedAt), a.UptoGlobalPosition, a.StreamCount, a.RootHash,
	); err != nil {
		return es.Anchor{}, fmt.Errorf("insert anchor: %w", err)
	}
	return a, nil
}

// ListAnchors implements es.Anchorer.
func (s *Store) ListAnchors(ctx context.Context) ([]es.Anchor, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT anchor_id, created_at, upto_global_position, stream_count, root_hash FROM anchors ORDER BY upto_global_position, created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []es.Anchor
	for rows.Next() {
		var (
			a       es.Anchor
			created string
		)
		if err := rows.Scan(&a.ID, &created, &a.UptoGlobalPosition, &a.StreamCount, &a.RootHash); err != nil {
			return nil, err
		}
		if a.CreatedAt, err = parseTS(created); err != nil {
			return nil, fmt.Errorf("parse created_at %q: %w", created, err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// CheckAnchor implements es.Anchorer: recomputes the root at the anchor's
// position and compares. A recorded bound of 0 is the literal empty
// bound — the only resolved bound an anchor over an empty log can
// carry, positions starting at 1 — and is verified as the empty log
// forever: heads(0) is provably empty (no re-resolution exists on the
// single-writer store), and no order row is covered. A new-style anchor
// (non-nil OrderRootHash) has its three order fields compared over the
// same bounded section; a legacy anchor (nil) skips them and verifies
// heads-only.
func (s *Store) CheckAnchor(ctx context.Context, a es.Anchor) error {
	heads, err := s.heads(ctx, a.UptoGlobalPosition)
	if err != nil {
		return err
	}
	root := es.AnchorRoot(heads)
	if uint64(len(heads)) != a.StreamCount || !bytes.Equal(root[:], a.RootHash) {
		return fmt.Errorf("%w: anchor %s at global_position %d (streams %d, recomputed %d)",
			es.ErrAnchorMismatch, a.ID, a.UptoGlobalPosition, a.StreamCount, len(heads))
	}
	// The order fields compare only on a new-style anchor: nil
	// OrderRootHash is exclusively the legacy marker for an anchor
	// recorded before log-order attestation, and such an anchor
	// verifies heads-only.
	if a.OrderRootHash == nil {
		return nil
	}
	// The covered order rows are exactly those with to_position <= the
	// recorded bound — and none at the empty bound, a row's
	// from_position being at least 1. orderRows' own upto == 0
	// convention is the whole-log read of OrderRows(0), which the empty
	// bound must not borrow: the read is skipped and rows stays nil.
	var rows []es.OrderRow
	if a.UptoGlobalPosition > 0 {
		rows, err = s.orderRows(ctx, a.UptoGlobalPosition)
		if err != nil {
			return err
		}
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

// ListStreams implements es.Anchorer. SQLite is single-workspace, so the
// Workspace of every ref is empty.
func (s *Store) ListStreams(ctx context.Context) ([]es.StreamRef, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT stream_id FROM events ORDER BY stream_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []es.StreamRef
	for rows.Next() {
		var canonical string
		if err := rows.Scan(&canonical); err != nil {
			return nil, err
		}
		sid, err := es.ParseCanonical(canonical)
		if err != nil {
			return nil, err
		}
		out = append(out, es.StreamRef{StreamID: sid})
	}
	return out, rows.Err()
}

// OrderRows implements es.Anchorer: the log_order rows with
// to_position <= upto. upto == 0 reads the whole log — which is exactly
// "the current log head": every committed row's to_position was
// assigned under the single writer, so no committed row can sit above
// it. The read takes no fence round because it records nothing (the
// single-writer lock keeps committed state stable, adjudication §2.1);
// the paths that freeze rows into a root (Anchor, CheckAnchor,
// VerifyOrder) read them through the same helper under their own
// resolved bound.
func (s *Store) OrderRows(ctx context.Context, upto uint64) ([]es.OrderRow, error) {
	return s.orderRows(ctx, upto)
}

// logOrderRow is one log_order row with the scope column es.OrderRow
// drops — the canonical stream — which the deep verify needs to group
// events to their batch.
type logOrderRow struct {
	es.OrderRow
	Stream string // canonical "type:id", as events stores it

	// entries are the events VerifyOrder found in the row's range, in
	// the position order the pages delivered them (ascending).
	entries []es.OrderEntry
}

// readLogOrderRows reads the log_order rows with to_position <= upto
// (upto == 0: no clamp, the whole log) in ascending from_position
// order.
func (s *Store) readLogOrderRows(ctx context.Context, upto uint64) ([]logOrderRow, error) {
	q := `SELECT from_position, to_position, event_count, batch_hash, stream_id FROM log_order`
	args := []any{}
	if upto > 0 {
		q += ` WHERE to_position <= ?`
		args = append(args, upto)
	}
	q += ` ORDER BY from_position`
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("read log_order: %w", err)
	}
	defer rows.Close()
	var out []logOrderRow
	for rows.Next() {
		var r logOrderRow
		if err := rows.Scan(&r.From, &r.To, &r.Count, &r.BatchHash, &r.Stream); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// orderRows adapts readLogOrderRows to the es.OrderRow shape the
// anchor paths aggregate.
func (s *Store) orderRows(ctx context.Context, upto uint64) ([]es.OrderRow, error) {
	full, err := s.readLogOrderRows(ctx, upto)
	if err != nil {
		return nil, err
	}
	out := make([]es.OrderRow, len(full))
	for i := range full {
		out[i] = full[i].OrderRow
	}
	return out, nil
}

// orderVerifyPageSize is the event page size VerifyOrder reads
// positions at: large enough that verifying a big log is a few round
// trips per thousand events, small enough that no page's payloads
// stress memory (the cost class is VerifyStream's, which also reads
// full envelopes).
const orderVerifyPageSize = 1000

// readPositions pages events by global position: the envelopes with
// global_position > from and <= to, ascending, at most limit of them.
// It reuses envelopeCols — the column shape every envelope read shares
// — so the deep verify walks exactly what the store returns everywhere
// else.
func (s *Store) readPositions(ctx context.Context, from, to uint64, limit int) ([]es.Envelope, error) {
	q := `SELECT ` + envelopeCols + ` FROM events
	      WHERE global_position > ? AND global_position <= ?
	      ORDER BY global_position ASC
	      LIMIT ?`
	return s.queryEnvelopes(ctx, q, from, to, limit)
}

// coveringRow returns the row of rs whose [From, To] range contains
// pos, or nil. rs is from_position ascending; the rightmost row
// starting at or below pos is the only candidate, and if its range does
// not reach pos, no row does. (A tampered log can break the
// disjointness — overlapping rows, positions moved across streams — and
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
// F11): for every log_order row at or below the bound it recomputes
// the row's es.BatchOrderHash from the events themselves — paged by
// position, grouped to each row by its stream scope and its
// [from_position, to_position] range — and reports the first row whose
// recomputed hash disagrees with the stored batch_hash. This is the
// check that catches a repositioned, inserted or deleted event inside
// an attested batch, which the per-stream chains and the heads root
// all survive; the exported anchor's OrderRootHash is the off-box
// witness that catches a wholesale rewrite (ADR 0029's chain/anchor
// trust split, applied to order).
//
// No fence round is needed (adjudication §2.1): the store's single
// connection serializes every append behind every read, and the bound
// excludes everything later — a batch's row and its events commit in
// one transaction, and any append that starts after this verification
// begins draws positions strictly above the bound, so nothing at or
// below it can appear mid-verification and the verdict cannot be torn.
//
// Events appended before migration 0006 have no log_order row; they
// are unattested and skipped (the order analog of the chain's
// ChainedFrom).
func (s *Store) VerifyOrder(ctx context.Context, upto uint64) (*es.OrderVerifyReport, error) {
	if upto == 0 {
		if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(global_position), 0) FROM events`).Scan(&upto); err != nil {
			return nil, fmt.Errorf("read log head: %w", err)
		}
	}
	rows, err := s.readLogOrderRows(ctx, upto)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return &es.OrderVerifyReport{OK: true}, nil
	}

	// Index the rows by stream scope, keeping the global from_position
	// order per scope; rows are referenced in place so the entries each
	// page adds land on the row being verified. SQLite is
	// single-workspace (0001's rule), so the canonical stream alone is
	// the scope — the constant-workspace half of Postgres's
	// (workspace, stream) key.
	byScope := make(map[string][]*logOrderRow)
	for i := range rows {
		r := &rows[i]
		byScope[r.Stream] = append(byScope[r.Stream], r)
	}

	// Page the log from the first covered position — nothing below it
	// can belong to a row — and group each event to its row. Pages
	// arrive ascending by position, so every row's entries do too: the
	// order BatchOrderHash attests.
	cursor := rows[0].From - 1
	for {
		page, err := s.readPositions(ctx, cursor, upto, orderVerifyPageSize)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			break
		}
		for i := range page {
			e := &page[i]
			cursor = e.GlobalPosition
			if r := coveringRow(byScope[e.StreamID.Canonical()], e.GlobalPosition); r != nil {
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
	rep := &es.OrderVerifyReport{OK: true, RowsChecked: uint64(len(rows))}
	for i := range rows {
		recomputed := es.BatchOrderHash(rows[i].entries)
		if !bytes.Equal(recomputed[:], rows[i].BatchHash) {
			rep.OK = false
			rep.Mismatch = &es.OrderRowMismatch{
				From:              rows[i].From,
				ExpectedBatchHash: append([]byte(nil), rows[i].BatchHash...),
				ActualBatchHash:   append([]byte(nil), recomputed[:]...),
			}
			break
		}
	}
	return rep, nil
}

// heads returns each stream's highest version at or below upto with its hash.
func (s *Store) heads(ctx context.Context, upto uint64) ([]es.StreamHead, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT e.stream_id, e.version, e.hash
		   FROM events e
		   JOIN (SELECT stream_id, MAX(version) AS v FROM events WHERE global_position <= ? GROUP BY stream_id) m
		     ON m.stream_id = e.stream_id AND m.v = e.version
		  ORDER BY e.stream_id`, upto)
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
		if err := rows.Scan(&canonical, &h.Version, &h.Hash); err != nil {
			return nil, err
		}
		if h.StreamID, err = es.ParseCanonical(canonical); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}
