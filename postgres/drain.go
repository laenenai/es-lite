package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/keystore"
	"github.com/laenenai/es-lite/shred"
)

// Drain is the gap-safe delivery relay for Postgres (ADR 0001 §5). Unlike a
// monotonic cursor — which can skip an event whose global_position committed
// out of assignment order — it claims unpublished rows with FOR UPDATE SKIP
// LOCKED, publishes them, and marks them published, all in one transaction.
// Publishing before commit makes it at-least-once: a crash after publish but
// before commit re-delivers the batch (dedup on the consumer side, ADR 0003).
//
// It reads across all workspaces (RLS bypassed) and decrypts each event with
// its workspace DEK. Events whose workspace has been shredded cannot be
// decrypted; they are excluded from the batch but still marked published so
// they are not re-claimed forever. Returns the number of rows claimed
// (including skipped-shredded), so a caller can loop until it returns 0.
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

	// Decrypt with a per-workspace cipher cache. A nil entry marks a
	// shredded workspace whose events must be skipped.
	ciphers := map[string]*shred.Cipher{}
	fetched := map[string]bool{}
	var (
		batch     []es.Envelope
		positions []uint64
		claimed   int
	)
	// Materialize rows first (can't call cipher()/new queries while rows open).
	type rawRow struct {
		env         es.Envelope
		wsID        string
		ciphertext  []byte
	}
	var raws []rawRow
	for rows.Next() {
		var (
			r                                     rawRow
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
		r.env, r.wsID, r.ciphertext = e, wsID, payload
		raws = append(raws, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()

	for _, r := range raws {
		claimed++
		positions = append(positions, r.env.GlobalPosition)

		// Plaintext mode (no keystore wired): payload is stored as-is.
		if s.shredder == nil {
			env := r.env
			env.Payload = r.ciphertext
			batch = append(batch, env)
			continue
		}

		if !fetched[r.wsID] {
			fetched[r.wsID] = true
			cip, err := s.readCipher(ctx, r.wsID)
			if err != nil {
				if errors.Is(err, keystore.ErrShredded) {
					ciphers[r.wsID] = nil // shredded: skip this workspace's events
				} else {
					return 0, fmt.Errorf("cipher %s: %w", r.wsID, err)
				}
			} else {
				ciphers[r.wsID] = cip
			}
		}
		cipher := ciphers[r.wsID]
		if cipher == nil {
			continue // shredded workspace
		}
		pt, err := cipher.Decrypt(r.ciphertext)
		if err != nil {
			// Undecryptable (mid-shred race): skip but still mark published.
			continue
		}
		env := r.env
		env.Payload = pt
		batch = append(batch, env)
	}

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
