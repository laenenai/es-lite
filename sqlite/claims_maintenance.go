package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/laenenai/es-lite/es"
)

// The claims index is derived state (ADR 0043): the append path keeps
// unique_claims authoritative and records each append's constraint ops on its
// first event row, so these two operations can reconstruct the index after a
// restore and prove a live index matches its log. Each runs in one transaction
// and commits once, so the guard, the delete, the replay and the final count
// all see the same snapshot; a decode or apply failure rolls the whole
// operation back.
//
// One SQLite database is one workspace, so unlike the Postgres adapter these
// carry no workspace predicate and no row security: every row in the database
// belongs to the single tenant.

var _ es.ClaimsMaintainer = (*Store)(nil)

// opPageSize bounds the op-bearing event rows one replay query buffers. SQLite
// keeps one connection, so a page is read and closed before its ops are
// applied; keyset paging on global_position replays in the one order the log
// defines without holding a large log in memory all at once.
const opPageSize = 1000

// RebuildClaims replaces the live claims index by replaying every op-bearing
// event in global position order (ADR 0043 §2.2). It refuses an inconsistent
// state — live claims with no ops in the event log — returning
// ErrClaimsWithoutDecisions without deleting anything, because the log cannot
// reconstruct that index.
func (s *Store) RebuildClaims(ctx context.Context) (es.ClaimsRebuildReport, error) {
	var zero es.ClaimsRebuildReport
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return zero, fmt.Errorf("sqlite: rebuild claims: begin: %w", err)
	}
	defer tx.Rollback()

	claims, err := countRows(ctx, tx, `SELECT count(*) FROM unique_claims`)
	if err != nil {
		return zero, fmt.Errorf("sqlite: rebuild claims: count live claims: %w", err)
	}
	ops, err := countRows(ctx, tx, `SELECT count(*) FROM events WHERE constraint_ops IS NOT NULL`)
	if err != nil {
		return zero, fmt.Errorf("sqlite: rebuild claims: count ops: %w", err)
	}
	if ops == 0 && claims > 0 {
		// A live index with no op-bearing events is inconsistent: a partial
		// restore, or rows written outside the append path. The log cannot
		// reconstruct it, so refuse rather than guess or delete (ADR 0043
		// §2.5).
		return zero, es.ErrClaimsWithoutDecisions
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM unique_claims`); err != nil {
		return zero, fmt.Errorf("sqlite: rebuild claims: clear index: %w", err)
	}
	replayed, err := replayOps(ctx, tx, "unique_claims")
	if err != nil {
		return zero, err
	}
	after, err := countRows(ctx, tx, `SELECT count(*) FROM unique_claims`)
	if err != nil {
		return zero, fmt.Errorf("sqlite: rebuild claims: count rebuilt claims: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return zero, fmt.Errorf("sqlite: rebuild claims: commit: %w", err)
	}
	return es.ClaimsRebuildReport{Ops: replayed, Claims: after}, nil
}

// VerifyClaims rebuilds the event log's ops into a temporary copy of the index
// and reports the first (scope, value, stream) where the copy and the live
// index disagree (ADR 0043 §2.2). It never modifies the live index; the temp
// table is dropped by the next verify and otherwise lives only on the store's
// single connection. Like RebuildClaims it refuses an inconsistent state —
// live claims with no ops in the event log — returning
// ErrClaimsWithoutDecisions instead of reporting the missing log as a
// divergence.
//
// A failed read or decode is an error, never an OK report: silence from a
// broken read would be indistinguishable from a matching index.
func (s *Store) VerifyClaims(ctx context.Context) (es.ClaimsVerifyReport, error) {
	var zero es.ClaimsVerifyReport
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return zero, fmt.Errorf("sqlite: verify claims: begin: %w", err)
	}
	defer tx.Rollback()

	// The guard mirrors RebuildClaims: live claims with no op-bearing events
	// are inconsistent — a partial restore or out-of-band SQL — so the log
	// cannot license (or reconstruct) the index. Refuse with
	// ErrClaimsWithoutDecisions rather than invent a divergence out of the
	// missing history (ADR 0043 §2.5).
	liveClaims, err := countRows(ctx, tx, `SELECT count(*) FROM unique_claims`)
	if err != nil {
		return zero, fmt.Errorf("sqlite: verify claims: count live claims: %w", err)
	}
	liveOps, err := countRows(ctx, tx, `SELECT count(*) FROM events WHERE constraint_ops IS NOT NULL`)
	if err != nil {
		return zero, fmt.Errorf("sqlite: verify claims: count ops: %w", err)
	}
	if liveOps == 0 && liveClaims > 0 {
		return zero, es.ErrClaimsWithoutDecisions
	}

	// SQLite has no ON COMMIT DROP, and the Store keeps one connection for
	// its lifetime, so a successful verify leaves the temp table behind on
	// that connection. Drop any earlier copy before rebuilding into it: a
	// reused table must never leak a previous run's rows into this diff.
	if _, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS temp.claims_rebuild`); err != nil {
		return zero, fmt.Errorf("sqlite: verify claims: drop temp index: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		CREATE TEMP TABLE claims_rebuild (
		    scope      TEXT NOT NULL,
		    value_key  BLOB NOT NULL,
		    stream_id  TEXT NOT NULL,
		    claimed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		    PRIMARY KEY (scope, value_key)
		)`); err != nil {
		return zero, fmt.Errorf("sqlite: verify claims: create temp index: %w", err)
	}
	ops, err := replayOps(ctx, tx, "claims_rebuild")
	if err != nil {
		return zero, err
	}
	claims, err := countRows(ctx, tx, `SELECT count(*) FROM claims_rebuild`)
	if err != nil {
		return zero, fmt.Errorf("sqlite: verify claims: count rebuilt claims: %w", err)
	}

	rep := es.ClaimsVerifyReport{OK: true, Ops: ops, Claims: claims}
	var (
		scope  string
		value  []byte
		stream string
	)
	// SQLite does not accept parenthesized operands in a compound SELECT, so
	// each EXCEPT branch is a parenthesized subquery: the same two-direction
	// diff, still with the branches isolated from UNION ALL's precedence.
	err = tx.QueryRowContext(ctx, `
		SELECT * FROM (
		    SELECT scope, value_key, stream_id FROM unique_claims
		    EXCEPT
		    SELECT scope, value_key, stream_id FROM claims_rebuild
		)
		UNION ALL
		SELECT * FROM (
		    SELECT scope, value_key, stream_id FROM claims_rebuild
		    EXCEPT
		    SELECT scope, value_key, stream_id FROM unique_claims
		)
		LIMIT 1`).Scan(&scope, &value, &stream)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// The two directions agree: the rebuilt index is exactly the live one.
	case err != nil:
		return zero, fmt.Errorf("sqlite: verify claims: diff: %w", err)
	default:
		rep.OK = false
		rep.FirstDivergence = fmt.Sprintf("scope=%s value=%x stream=%s", scope, value, stream)
	}

	if err := tx.Commit(); err != nil {
		return zero, fmt.Errorf("sqlite: verify claims: commit: %w", err)
	}
	return rep, nil
}

// replayOps applies every op-bearing event to table in global position order,
// returning how many op records it replayed. Paging is keyset on
// global_position: a page is read and closed before its ops are applied, and
// the next page starts strictly above the last position read. The ops go
// through the same DecodeConstraintOps and applyClaims the append path uses,
// so release-then-claim ordering is identical to a live append.
func replayOps(ctx context.Context, tx *sql.Tx, table string) (int64, error) {
	type opRecord struct {
		position uint64
		streamID string
		ops      []byte
	}
	var (
		applied int64
		after   uint64
	)
	for {
		rows, err := tx.QueryContext(ctx,
			`SELECT global_position, stream_id, constraint_ops FROM events
			  WHERE constraint_ops IS NOT NULL AND global_position > ?
			  ORDER BY global_position LIMIT ?`, after, opPageSize)
		if err != nil {
			return applied, fmt.Errorf("sqlite: replay ops: read: %w", err)
		}
		var page []opRecord
		for rows.Next() {
			var r opRecord
			if err := rows.Scan(&r.position, &r.streamID, &r.ops); err != nil {
				rows.Close()
				return applied, fmt.Errorf("sqlite: replay ops: scan: %w", err)
			}
			page = append(page, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return applied, fmt.Errorf("sqlite: replay ops: read: %w", err)
		}
		if len(page) == 0 {
			return applied, nil
		}
		for _, r := range page {
			ops, err := es.DecodeConstraintOps(r.ops)
			if err != nil {
				return applied, fmt.Errorf("sqlite: replay ops at position %d: %w", r.position, err)
			}
			if err := applyClaims(ctx, tx, table, r.streamID, ops); err != nil {
				return applied, err
			}
			applied++
			after = r.position
		}
	}
}

// countRows runs one count query in the caller's transaction and returns it.
func countRows(ctx context.Context, tx *sql.Tx, query string) (int64, error) {
	var n int64
	if err := tx.QueryRowContext(ctx, query).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}
