package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/laenenai/es-lite/es"
)

// The claims index is derived state (ADR 0043): the append path keeps
// unique_claims authoritative and records each append's constraint ops in
// append_decisions, so these two operations can reconstruct the index after a
// restore and prove a live index matches its log. Each runs in one transaction
// with app.workspace_id set, and every statement carries the explicit
// workspace_id predicate — row security is the second line, not the only one
// (the login role may be exempt from it).

var _ es.ClaimsMaintainer = (*Workspace)(nil)

// decisionPageSize bounds the append_decisions rows one replay query
// buffers. pgx refuses a second statement on a connection while rows are
// open, so a page is read and closed before its ops are applied; paging
// keeps a large log from being held in memory all at once.
const decisionPageSize = 1000

// RebuildClaims replaces the live claims index by replaying every decision
// record in append order (ADR 0043 §3.2). It refuses an inconsistent state —
// live claims with no decision history — returning ErrClaimsWithoutDecisions
// without deleting anything, because the log cannot reconstruct that index.
//
// One transaction, one commit: the guard, the delete, the replay and the
// final count all see the same snapshot, and a decode or apply failure
// rolls the whole rebuild back, leaving the live index as it was.
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
	decisions, err := countRows(ctx, tx,
		`SELECT count(*) FROM append_decisions WHERE workspace_id = $1`, w.ws)
	if err != nil {
		return zero, fmt.Errorf("postgres: rebuild claims: count decisions: %w", err)
	}
	if decisions == 0 && claims > 0 {
		// A live index with no decision history is inconsistent: a partial
		// restore, or rows written outside the append path. The log cannot
		// reconstruct it, so refuse rather than guess or delete (ADR 0043
		// §3.5).
		return zero, es.ErrClaimsWithoutDecisions
	}

	if _, err := tx.Exec(ctx,
		`DELETE FROM unique_claims WHERE workspace_id = $1`, w.ws); err != nil {
		return zero, fmt.Errorf("postgres: rebuild claims: clear index: %w", err)
	}
	replayed, err := w.replayDecisions(ctx, tx, "unique_claims")
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
	return es.ClaimsRebuildReport{Decisions: replayed, Claims: after}, nil
}

// VerifyClaims rebuilds the decision log into a temporary copy of the index
// and reports the first (scope, value, stream) where the copy and the live
// index disagree (ADR 0043 §3.2). It never modifies the live index; the temp
// table drops with the transaction. Like RebuildClaims it refuses an
// inconsistent state — live claims with no decision history — returning
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

	// The guard mirrors RebuildClaims: live claims with no decision history
	// are inconsistent — a partial restore or out-of-band SQL — so the log
	// cannot license (or reconstruct) the index. Refuse with
	// ErrClaimsWithoutDecisions rather than invent a divergence out of the
	// missing history (ADR 0043 §3.5).
	liveClaims, err := countRows(ctx, tx,
		`SELECT count(*) FROM unique_claims WHERE workspace_id = $1`, w.ws)
	if err != nil {
		return zero, fmt.Errorf("postgres: verify claims: count live claims: %w", err)
	}
	liveDecisions, err := countRows(ctx, tx,
		`SELECT count(*) FROM append_decisions WHERE workspace_id = $1`, w.ws)
	if err != nil {
		return zero, fmt.Errorf("postgres: verify claims: count decisions: %w", err)
	}
	if liveDecisions == 0 && liveClaims > 0 {
		return zero, es.ErrClaimsWithoutDecisions
	}

	// LIKE INCLUDING ALL copies the columns, defaults, constraints and the
	// unique index; row security is not copied, and the explicit workspace
	// predicate below scopes every read regardless.
	if _, err := tx.Exec(ctx,
		`CREATE TEMP TABLE claims_rebuild (LIKE unique_claims INCLUDING ALL) ON COMMIT DROP`); err != nil {
		return zero, fmt.Errorf("postgres: verify claims: create temp index: %w", err)
	}
	decisions, err := w.replayDecisions(ctx, tx, "claims_rebuild")
	if err != nil {
		return zero, err
	}
	claims, err := countRows(ctx, tx,
		`SELECT count(*) FROM claims_rebuild WHERE workspace_id = $1`, w.ws)
	if err != nil {
		return zero, fmt.Errorf("postgres: verify claims: count rebuilt claims: %w", err)
	}

	rep := es.ClaimsVerifyReport{OK: true, Decisions: decisions, Claims: claims}
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

// replayDecisions applies every decision record for the workspace to table in
// append (id) order, returning how many rows it replayed. The ops go through
// the same DecodeDecisionOps and applyOps the append path uses, so release-
// then-claim ordering is identical to a live append.
func (w *Workspace) replayDecisions(ctx context.Context, tx DBTX, table string) (int64, error) {
	type decision struct {
		id       int64
		streamID string
		ops      []byte
	}
	var (
		applied int64
		after   int64
	)
	for {
		rows, err := tx.Query(ctx,
			`SELECT id, stream_id, ops FROM append_decisions
			  WHERE workspace_id = $1 AND id > $2 ORDER BY id LIMIT $3`,
			w.ws, after, decisionPageSize)
		if err != nil {
			return applied, fmt.Errorf("postgres: replay decisions: read: %w", err)
		}
		var page []decision
		for rows.Next() {
			var d decision
			if err := rows.Scan(&d.id, &d.streamID, &d.ops); err != nil {
				rows.Close()
				return applied, fmt.Errorf("postgres: replay decisions: scan: %w", err)
			}
			page = append(page, d)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return applied, fmt.Errorf("postgres: replay decisions: read: %w", err)
		}
		if len(page) == 0 {
			return applied, nil
		}
		for _, d := range page {
			ops, err := es.DecodeDecisionOps(d.ops)
			if err != nil {
				return applied, fmt.Errorf("postgres: replay decision %d: %w", d.id, err)
			}
			if err := w.applyOps(ctx, tx, table, d.streamID, ops); err != nil {
				return applied, err
			}
			applied++
			after = d.id
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
