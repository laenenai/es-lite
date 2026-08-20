package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/es-lite/es"
)

// Drain is the gap-safe delivery relay for Postgres (ADR 0001 §5). Unlike a
// monotonic cursor — which can skip an event whose global_position committed
// out of assignment order — it claims unpublished rows with FOR UPDATE SKIP
// LOCKED, publishes them, and marks them published, all in one transaction.
// Publishing before commit makes it at-least-once: a crash after publish but
// before commit re-delivers the batch (dedup on the consumer side, ADR 0003).
//
// It reads across all workspaces (RLS bypassed). Payloads are relayed opaquely
// (ADR 0025) — es-lite performs no decryption. Returns the number of rows
// claimed, so a caller can loop until it returns 0.
func (s *Store) Drain(ctx context.Context, limit int, publish func(context.Context, []es.Envelope) error) (int, error) {
	if limit <= 0 {
		limit = 100
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
		var (
			e                                     es.Envelope
			eventID, streamType, streamID         string
			correlationID, causationID, commandID string
			actorType, actorID                    string
			payload                               []byte
			wsID                                  string
		)
		if err := rows.Scan(
			&e.GlobalPosition, &eventID, &streamType, &streamID, &e.Version,
			&e.TypeURL, &e.SchemaVersion, &e.OccurredAt, &e.RecordedAt,
			&correlationID, &causationID, &commandID,
			&actorType, &actorID, &payload, &wsID,
		); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan: %w", err)
		}
		sid, err := es.ParseCanonical(streamID)
		if err != nil {
			rows.Close()
			return 0, err
		}
		e.StreamID = sid
		e.EventID, _ = uuid.Parse(eventID)
		e.CorrelationID, _ = uuid.Parse(correlationID)
		e.CausationID, _ = uuid.Parse(causationID)
		e.CommandID, _ = uuid.Parse(commandID)
		e.Actor = es.Actor{Type: actorType, ID: actorID}
		e.Workspace = wsID
		e.Payload = payload // opaque bytes, relayed unchanged (ADR 0025)
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
	if _, err := tx.Exec(ctx,
		`UPDATE events SET published_at = $1 WHERE global_position = ANY($2)`,
		time.Now().UTC(), positions,
	); err != nil {
		return 0, fmt.Errorf("mark published: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return claimed, nil
}
