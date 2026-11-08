package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/laenenai/es-lite/es"
)

// ErrRelayRoleMissing reports that the session user reaching for the RLS
// bypass — Drain, or the anchor path's bypassTx — is not a member of the
// es_relay role, so the bypass is denied to it (migration 0005):
// continuing would silently claim or read zero rows (and Anchor would
// record a root over zero heads as a real anchor). Both paths fail fast
// with this error instead. The remedy — create es_relay and grant it to
// every bypass principal — is in the runbook (docs/deploy/README.md).
var ErrRelayRoleMissing = errors.New("session user is not a member of the es_relay role; the RLS bypass is role-gated (see docs/deploy/README.md)")

// relayRoleProbeTimeout bounds the es_relay membership probe, which runs
// decoupled from the caller's context (context.WithoutCancel): the
// verdict must not depend on that context, but the probe also holds
// relayRoleMu, so an unbounded one would pin every bypass caller for as
// long as the pool stalls.
const relayRoleProbeTimeout = 5 * time.Second

// checkRelayRole is the startup check every RLS-bypass path shares — Drain
// and the anchor path's bypassTx (spec §5.7): migration 0005 gates the
// events and unique_claims bypass arm on es_relay membership, so a
// session outside the role would set app.bypass_rls and silently read
// nothing — a delivery outage dressed up as an empty backlog, or a wrong
// anchor. A definitive verdict is probed once per Store and stands for
// the Store's lifetime: a member stays cleared, and a refused process
// keeps failing loudly until the runbook's GRANT lands and it is
// restarted. A probe failure, though, is not a verdict — a cancelled
// context, a flaky connection — so it is returned but not cached, and
// the next call re-probes instead of inheriting the blip forever (F4).
// The probe runs on context.WithoutCancel(ctx) with its own timeout on
// purpose: its outcome must not depend on the caller's context — a
// caller with a cancelled ctx must neither poison the verdict nor skip
// the probe. It runs before the bypass is set, so a refused session
// claims and records nothing.
func (s *Store) checkRelayRole(ctx context.Context) error {
	s.relayRoleMu.Lock()
	defer s.relayRoleMu.Unlock()
	if s.relayRoleChecked {
		// Only definitive verdicts are cached, so this stands until the
		// process restarts: a member stays cleared, a non-member keeps
		// failing loudly.
		if s.relayRoleMember {
			return nil
		}
		return fmt.Errorf("postgres: drain: %w", ErrRelayRoleMissing)
	}
	// A missing es_relay is the loudest form of missing membership,
	// and pg_has_role errors on a nonexistent role — so the EXISTS
	// arm keeps the verdict false instead. CASE evaluates in order
	// (unlike AND/OR, whose short-circuiting is not guaranteed).
	probeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), relayRoleProbeTimeout)
	defer cancel()
	var member bool
	if err := s.pool.QueryRow(probeCtx,
		`SELECT CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'es_relay')
		             THEN pg_has_role(current_user, 'es_relay', 'member')
		             ELSE false END`,
	).Scan(&member); err != nil {
		// Transient, not a verdict: nothing is cached, so the next
		// call re-probes.
		return fmt.Errorf("postgres: drain: es_relay membership check: %w", err)
	}
	// The probe answered: definitive either way, so cache the verdict.
	s.relayRoleChecked = true
	s.relayRoleMember = member
	if !member {
		return fmt.Errorf("postgres: drain: %w", ErrRelayRoleMissing)
	}
	return nil
}

// Drain is the gap-safe delivery relay for Postgres (ADR 0001 §5). Unlike a
// monotonic cursor — which can skip an event whose global_position committed
// out of assignment order — it claims unpublished rows with FOR UPDATE SKIP
// LOCKED, publishes them, and marks them published, all in one transaction.
// Publishing before commit makes it at-least-once: a crash after publish but
// before commit re-delivers the batch (dedup on the consumer side, ADR 0036).
//
// It reads across all workspaces (RLS bypassed — and role-gated since
// migration 0005: a session whose user is not an es_relay member fails fast
// with ErrRelayRoleMissing instead of silently reading nothing). Payloads
// are relayed opaquely (ADR 0025) — es-lite performs no decryption. Returns
// the number of rows claimed, so a caller can loop until it returns 0.
func (s *Store) Drain(ctx context.Context, limit int, publish func(context.Context, []es.Envelope) error) (int, error) {
	if limit <= 0 {
		limit = 100
	}
	if err := s.checkRelayRole(ctx); err != nil {
		return 0, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT set_config('app.bypass_rls', 'on', true)`); err != nil {
		return 0, fmt.Errorf("set bypass: %w", err)
	}

	rows, err := tx.Query(ctx,
		`SELECT `+envelopeCols+` FROM events
		 WHERE published_at IS NULL
		 ORDER BY global_position ASC
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, fmt.Errorf("claim: %w", err)
	}

	var (
		batch     []es.Envelope
		positions []uint64
		claimed   int
	)
	for rows.Next() {
		e, err := scanEnvelope(rows)
		if err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan: %w", err)
		}
		claimed++
		positions = append(positions, e.GlobalPosition)
		batch = append(batch, e)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()

	if claimed == 0 {
		return 0, nil
	}
	if len(batch) > 0 {
		if err := publish(ctx, batch); err != nil {
			return 0, fmt.Errorf("publish: %w", err)
		}
	}
	// published_at is the database's own clock (clock_timestamp() in
	// SQL, F33), never the relay process's: a relay skewed against the
	// database cannot stamp deliveries outside the server's timeline,
	// and every replica draining this store shares one monotone clock.
	if _, err := tx.Exec(ctx,
		`UPDATE events SET published_at = clock_timestamp() WHERE global_position = ANY($1)`,
		positions,
	); err != nil {
		return 0, fmt.Errorf("mark published: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return claimed, nil
}
