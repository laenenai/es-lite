package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/laenenai/es-lite/es"
)

// The claims index is derived state (ADR 0043): the append path keeps
// unique_claims authoritative and records each append's constraint ops on its
// first event row, so these two operations can reconstruct the index after a
// restore and prove a live index matches its log. Each runs in one transaction
// with app.workspace_id set, and every statement carries the explicit
// workspace_id predicate — row security is the second line, not the only one
// (the login role may be exempt from it).

var _ es.ClaimsMaintainer = (*Workspace)(nil)

// opPageSize bounds the op-bearing event rows one replay query buffers. pgx
// refuses a second statement on a connection while rows are open, so a page is
// read and closed before its ops are applied; keyset paging on global_position
// keeps a large log from being held in memory all at once and replays in the
// one order the log defines.
const opPageSize = 1000

// RebuildClaims replaces the live claims index by replaying every op-bearing
// event in global position order (ADR 0043 §2.2). It refuses an inconsistent
// state — live claims with no ops in the event log — returning
// ErrClaimsWithoutDecisions without deleting anything, because the log cannot
// reconstruct that index.
//
// One transaction, one commit: the guard, the delete, the replay and the final
// count all see the same snapshot, and a decode or apply failure rolls the
// whole rebuild back, leaving the live index as it was.
func (w *Workspace) RebuildClaims(ctx context.Context) (es.ClaimsRebuildReport, error) {
	var zero es.ClaimsRebuildReport
	tx, err := w.store.pool.Begin(ctx)
	if err != nil {
		return zero, fmt.Errorf("postgres: rebuild claims: begin: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := setWorkspace(ctx, tx, w.ws); err != nil {
		return zero, fmt.Errorf("postgres: rebuild claims: set workspace: %w", err)
	}

	claims, err := countRows(ctx, tx,
		`SELECT count(*) FROM unique_claims WHERE workspace_id = $1`, w.ws)
	if err != nil {
		return zero, fmt.Errorf("postgres: rebuild claims: count live claims: %w", err)
	}
	ops, err := countRows(ctx, tx,
		`SELECT count(*) FROM events WHERE workspace_id = $1 AND constraint_ops IS NOT NULL`, w.ws)
	if err != nil {
		return zero, fmt.Errorf("postgres: rebuild claims: count ops: %w", err)
	}
	if ops == 0 && claims > 0 {
		// A live index with no op-bearing events is inconsistent: a partial
		// restore, or rows written outside the append path. The log cannot
		// reconstruct it, so refuse rather than guess or delete (ADR 0043
		// §2.5).
		return zero, es.ErrClaimsWithoutDecisions
	}

	if _, err := tx.Exec(ctx,
		`DELETE FROM unique_claims WHERE workspace_id = $1`, w.ws); err != nil {
		return zero, fmt.Errorf("postgres: rebuild claims: clear index: %w", err)
	}
	replayed, err := w.replayOps(ctx, tx, "unique_claims")
	if err != nil {
		return zero, err
	}
	after, err := countRows(ctx, tx,
		`SELECT count(*) FROM unique_claims WHERE workspace_id = $1`, w.ws)
	if err != nil {
		return zero, fmt.Errorf("postgres: rebuild claims: count rebuilt claims: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return zero, fmt.Errorf("postgres: rebuild claims: commit: %w", err)
	}
	return es.ClaimsRebuildReport{Ops: replayed, Claims: after}, nil
}

// VerifyClaims rebuilds the event log's ops into a temporary copy of the index
// and reports the first (scope, value, stream) where the copy and the live
// index disagree (ADR 0043 §2.2). It never modifies the live index; the temp
// table drops with the transaction. Like RebuildClaims it refuses an
// inconsistent state — live claims with no ops in the event log — returning
// ErrClaimsWithoutDecisions instead of reporting the missing log as a
// divergence.
//
// A failed read or decode is an error, never an OK report: silence from a
// broken read would be indistinguishable from a matching index.
func (w *Workspace) VerifyClaims(ctx context.Context) (es.ClaimsVerifyReport, error) {
	var zero es.ClaimsVerifyReport
	tx, err := w.store.pool.Begin(ctx)
	if err != nil {
		return zero, fmt.Errorf("postgres: verify claims: begin: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := setWorkspace(ctx, tx, w.ws); err != nil {
		return zero, fmt.Errorf("postgres: verify claims: set workspace: %w", err)
	}

	// The guard mirrors RebuildClaims: live claims with no op-bearing events
	// are inconsistent — a partial restore or out-of-band SQL — so the log
	// cannot license (or reconstruct) the index. Refuse with
	// ErrClaimsWithoutDecisions rather than invent a divergence out of the
	// missing history (ADR 0043 §2.5).
	liveClaims, err := countRows(ctx, tx,
		`SELECT count(*) FROM unique_claims WHERE workspace_id = $1`, w.ws)
	if err != nil {
		return zero, fmt.Errorf("postgres: verify claims: count live claims: %w", err)
	}
	liveOps, err := countRows(ctx, tx,
		`SELECT count(*) FROM events WHERE workspace_id = $1 AND constraint_ops IS NOT NULL`, w.ws)
	if err != nil {
		return zero, fmt.Errorf("postgres: verify claims: count ops: %w", err)
	}
	if liveOps == 0 && liveClaims > 0 {
		return zero, es.ErrClaimsWithoutDecisions
	}

	// LIKE INCLUDING ALL copies the columns, defaults, constraints and the
	// unique index; row security is not copied, and the explicit workspace
	// predicate below scopes every read regardless.
	if _, err := tx.Exec(ctx,
		`CREATE TEMP TABLE claims_rebuild (LIKE unique_claims INCLUDING ALL) ON COMMIT DROP`); err != nil {
		return zero, fmt.Errorf("postgres: verify claims: create temp index: %w", err)
	}
	ops, err := w.replayOps(ctx, tx, "claims_rebuild")
	if err != nil {
		return zero, err
	}
	claims, err := countRows(ctx, tx,
		`SELECT count(*) FROM claims_rebuild WHERE workspace_id = $1`, w.ws)
	if err != nil {
		return zero, fmt.Errorf("postgres: verify claims: count rebuilt claims: %w", err)
	}

	rep := es.ClaimsVerifyReport{OK: true, Ops: ops, Claims: claims}
	var (
		scope  string
		value  []byte
		stream string
	)
	err = tx.QueryRow(ctx,
		`(SELECT scope, value_key, stream_id FROM unique_claims WHERE workspace_id = $1
		  EXCEPT
		  SELECT scope, value_key, stream_id FROM claims_rebuild WHERE workspace_id = $2)
		 UNION ALL
		 (SELECT scope, value_key, stream_id FROM claims_rebuild WHERE workspace_id = $3
		  EXCEPT
		  SELECT scope, value_key, stream_id FROM unique_claims WHERE workspace_id = $4)
		 LIMIT 1`,
		w.ws, w.ws, w.ws, w.ws,
	).Scan(&scope, &value, &stream)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// The two directions agree: the rebuilt index is exactly the live one.
	case err != nil:
		return zero, fmt.Errorf("postgres: verify claims: diff: %w", err)
	default:
		rep.OK = false
		rep.FirstDivergence = fmt.Sprintf("scope=%s value=%x stream=%s", scope, value, stream)
	}

	if err := tx.Commit(ctx); err != nil {
		return zero, fmt.Errorf("postgres: verify claims: commit: %w", err)
	}
	return rep, nil
}

// replayOps applies every op-bearing event for the workspace to table in
// global position order, returning how many op records it replayed. The ops go
// through the same DecodeConstraintOps and applyOps the append path uses, so
// release-then-claim ordering is identical to a live append. Paging is keyset
// on global_position: a page is closed before its ops are applied, and the next
// page starts strictly above the last position read.
func (w *Workspace) replayOps(ctx context.Context, tx DBTX, table string) (int64, error) {
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
		rows, err := tx.Query(ctx,
			`SELECT global_position, stream_id, constraint_ops FROM events
			  WHERE workspace_id = $1 AND constraint_ops IS NOT NULL AND global_position > $2
			  ORDER BY global_position LIMIT $3`,
			w.ws, after, opPageSize)
		if err != nil {
			return applied, fmt.Errorf("postgres: replay ops: read: %w", err)
		}
		var page []opRecord
		for rows.Next() {
			var r opRecord
			if err := rows.Scan(&r.position, &r.streamID, &r.ops); err != nil {
				rows.Close()
				return applied, fmt.Errorf("postgres: replay ops: scan: %w", err)
			}
			page = append(page, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return applied, fmt.Errorf("postgres: replay ops: read: %w", err)
		}
		if len(page) == 0 {
			return applied, nil
		}
		for _, r := range page {
			ops, err := es.DecodeConstraintOps(r.ops)
			if err != nil {
				return applied, fmt.Errorf("postgres: replay ops at position %d: %w", r.position, err)
			}
			if err := w.applyOps(ctx, tx, table, r.streamID, ops); err != nil {
				return applied, err
			}
			applied++
			after = r.position
		}
	}
}

// countRows runs one count query and returns it. The caller sets the
// workspace and owns the transaction, and the query carries its own
// workspace predicate.
func countRows(ctx context.Context, db DBTX, query string, args ...any) (int64, error) {
	var n int64
	if err := db.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}
