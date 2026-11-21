package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/laenenai/es-lite/es"
)

// The claims index is derived state (ADR 0043): the append path keeps
// unique_claims authoritative and records each append's constraint ops in
// append_decisions, so these three operations can reconstruct the index
// after a restore, prove a live index matches its log, and perform the
// one-time cutover backfill. Each runs in one transaction and commits once,
// so the guard, the delete, the replay and the final count all see the same
// snapshot; a decode or apply failure rolls the whole operation back.
//
// One SQLite database is one workspace, so unlike the Postgres adapter these
// carry no workspace predicate and no row security: every row in the
// database belongs to the single tenant.

var _ es.ClaimsMaintainer = (*Store)(nil)

// RebuildClaims replaces the live claims index by replaying every decision
// record in append order (ADR 0043 §3.2). It refuses a pre-cutover state —
// live claims with no decision history — returning ErrClaimsNotAdopted
// without deleting anything, because the log cannot reconstruct that index.
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
	decisions, err := countRows(ctx, tx, `SELECT count(*) FROM append_decisions`)
	if err != nil {
		return zero, fmt.Errorf("sqlite: rebuild claims: count decisions: %w", err)
	}
	if decisions == 0 && claims > 0 {
		// The cutover never ran. Deleting the index now would leave nothing
		// to rebuild from, and the caller must adopt first (ADR 0043 §3.5).
		return zero, es.ErrClaimsNotAdopted
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM unique_claims`); err != nil {
		return zero, fmt.Errorf("sqlite: rebuild claims: clear index: %w", err)
	}
	replayed, err := replayDecisions(ctx, tx, "unique_claims")
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
	return es.ClaimsRebuildReport{Decisions: replayed, Claims: after}, nil
}

// VerifyClaims rebuilds the decision log into a temporary copy of the index
// and reports the first (scope, value, stream) where the copy and the live
// index disagree (ADR 0043 §3.2). It never modifies the live index; the temp
// table is dropped by the next verify and otherwise lives only on the
// store's single connection. Like RebuildClaims it refuses a pre-cutover
// state — live claims with no decision history — returning
// ErrClaimsNotAdopted instead of reporting the missing log as a divergence.
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

	// The pre-cutover guard mirrors RebuildClaims: live claims with no
	// decision history mean the one-time cutover never ran, so the log cannot
	// license (or reconstruct) the index. That is "not adopted", not a
	// tamper; reporting a divergence would fail the runbook's exit-2
	// contract and invent a divergence out of the missing history.
	liveClaims, err := countRows(ctx, tx, `SELECT count(*) FROM unique_claims`)
	if err != nil {
		return zero, fmt.Errorf("sqlite: verify claims: count live claims: %w", err)
	}
	liveDecisions, err := countRows(ctx, tx, `SELECT count(*) FROM append_decisions`)
	if err != nil {
		return zero, fmt.Errorf("sqlite: verify claims: count decisions: %w", err)
	}
	if liveDecisions == 0 && liveClaims > 0 {
		return zero, es.ErrClaimsNotAdopted
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
	decisions, err := replayDecisions(ctx, tx, "claims_rebuild")
	if err != nil {
		return zero, err
	}
	claims, err := countRows(ctx, tx, `SELECT count(*) FROM claims_rebuild`)
	if err != nil {
		return zero, fmt.Errorf("sqlite: verify claims: count rebuilt claims: %w", err)
	}

	rep := es.ClaimsVerifyReport{OK: true, Decisions: decisions, Claims: claims}
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

// AdoptClaims performs the one-time cutover backfill (ADR 0043 §3.5): one
// synthetic Claim decision per live index row, with an empty span and a NULL
// command_id, the claim's own stream, so that replaying the log into an empty
// index reproduces today's index. It refuses when any decision already exists
// — running twice would duplicate a Claim and make every rebuild fail — and
// returns the number of rows written.
func (s *Store) AdoptClaims(ctx context.Context) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("sqlite: adopt claims: begin: %w", err)
	}
	defer tx.Rollback()

	existing, err := countRows(ctx, tx, `SELECT count(*) FROM append_decisions`)
	if err != nil {
		return 0, fmt.Errorf("sqlite: adopt claims: count decisions: %w", err)
	}
	if existing > 0 {
		return 0, es.ErrClaimsAlreadyAdopted
	}

	// Read the whole index before writing: the inserts must not run while
	// the SELECT's rows are still open on the single connection. Index rows
	// are small — scope, key and stream — and the caller quiesced writers
	// for the cutover.
	type indexRow struct {
		scope, streamID string
		valueKey        []byte
	}
	var index []indexRow
	rows, err := tx.QueryContext(ctx,
		`SELECT scope, value_key, stream_id FROM unique_claims
		  ORDER BY scope, value_key`)
	if err != nil {
		return 0, fmt.Errorf("sqlite: adopt claims: read index: %w", err)
	}
	for rows.Next() {
		var r indexRow
		if err := rows.Scan(&r.scope, &r.valueKey, &r.streamID); err != nil {
			rows.Close()
			return 0, fmt.Errorf("sqlite: adopt claims: scan index: %w", err)
		}
		index = append(index, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("sqlite: adopt claims: read index: %w", err)
	}

	var adopted int64
	for _, r := range index {
		ops := es.EncodeDecisionOps([]es.ConstraintOp{es.Claim(r.scope, string(r.valueKey))})
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO append_decisions (stream_id, from_version, to_version, command_id, ops)
			 VALUES (?,0,0,NULL,?)`,
			r.streamID, ops,
		); err != nil {
			return 0, fmt.Errorf("sqlite: adopt claims: insert %s: %w", r.scope, err)
		}
		adopted++
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("sqlite: adopt claims: commit: %w", err)
	}
	return adopted, nil
}

// replayDecisions applies every decision record to table in append (id)
// order, returning how many rows it replayed. It reads the whole log before
// applying, because the single connection cannot execute an insert while the
// query's rows are still open. The ops go through the same DecodeDecisionOps
// and applyClaims the append path uses, so release-then-claim ordering is
// identical to a live append.
func replayDecisions(ctx context.Context, tx *sql.Tx, table string) (int64, error) {
	type decision struct {
		id       int64
		streamID string
		ops      []byte
	}
	var decisions []decision
	rows, err := tx.QueryContext(ctx,
		`SELECT id, stream_id, ops FROM append_decisions ORDER BY id`)
	if err != nil {
		return 0, fmt.Errorf("sqlite: replay decisions: read: %w", err)
	}
	for rows.Next() {
		var d decision
		if err := rows.Scan(&d.id, &d.streamID, &d.ops); err != nil {
			rows.Close()
			return 0, fmt.Errorf("sqlite: replay decisions: scan: %w", err)
		}
		decisions = append(decisions, d)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("sqlite: replay decisions: read: %w", err)
	}

	var applied int64
	for _, d := range decisions {
		ops, err := es.DecodeDecisionOps(d.ops)
		if err != nil {
			return applied, fmt.Errorf("sqlite: replay decision %d: %w", d.id, err)
		}
		if err := applyClaims(ctx, tx, table, d.streamID, ops); err != nil {
			return applied, err
		}
		applied++
	}
	return applied, nil
}

// countRows runs one count query in the caller's transaction and returns it.
func countRows(ctx context.Context, tx *sql.Tx, query string) (int64, error) {
	var n int64
	if err := tx.QueryRowContext(ctx, query).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}
