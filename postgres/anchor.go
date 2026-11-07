package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
// the row and returns it. Runs under app.bypass_rls.
//
// The bound is fenced: settledBound resolves upto to a permanently
// settled position before any head is read, so a concurrent append below
// the head cannot tear the root — the anchor is recorded either after
// that append settled (its row included) or below it (its position
// excluded), and CheckAnchor agrees forever after.
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
	a := es.Anchor{
		ID:                 newV7().String(),
		CreatedAt:          es.TruncateMicros(time.Now()),
		UptoGlobalPosition: upto,
		StreamCount:        uint64(len(heads)),
		RootHash:           root[:],
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
// below the anchor's bound, and an agreement holds.
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
