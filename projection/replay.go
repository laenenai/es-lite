// Package projection provides the read-model rebuild primitive (ADR 0005):
// replay the event log through the same handler the live consumer uses.
package projection

import (
	"context"

	"github.com/laenenai/es-lite/es"
)

// Source is the read side Replay pages over. es.Store and the workspace-scoped
// Postgres store both satisfy it. (For Postgres, use a workspace-scoped store
// to replay one workspace, or an admin reader for all.)
type Source interface {
	ReadAll(ctx context.Context, fromPosition uint64, limit int) ([]es.Envelope, error)
}

// Apply handles one ordered batch. It has the same shape as delivery.Handler,
// so a projection's live handler and its rebuild handler are the same
// function — they cannot drift.
type Apply func(ctx context.Context, batch []es.Envelope) error

// Replay pages src.ReadAll from `from` (exclusive) in global order, applying
// each batch, and returns the last global_position applied. This is the
// authoritative rebuild source — the DB log, not JetStream retention
// (ADR 0005). Apply must be idempotent; pair it with a Marker so a rebuild
// can cut over to live delivery at exactly the returned position.
func Replay(ctx context.Context, src Source, from uint64, batch int, apply Apply) (uint64, error) {
	if batch <= 0 {
		batch = 500
	}
	pos := from
	for {
		evs, err := src.ReadAll(ctx, pos, batch)
		if err != nil {
			return pos, err
		}
		if len(evs) == 0 {
			return pos, nil
		}
		if err := apply(ctx, evs); err != nil {
			return pos, err
		}
		pos = evs[len(evs)-1].GlobalPosition
		if len(evs) < batch {
			return pos, nil // short page => log exhausted
		}
	}
}

// Marker persists a read model's highest-applied global_position. This single
// value does double duty (ADR 0005): live delivery skips events whose position
// is <= the marker (idempotent at-least-once), and a rebuild resumes live
// delivery at exactly the marker (exact cutover). Implementations live beside
// the read model — the sqlite/postgres checkpoints table can serve as one.
type Marker interface {
	Load(ctx context.Context, projection string) (uint64, error)
	Save(ctx context.Context, projection string, position uint64) error
}
