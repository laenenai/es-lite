// Package snapshot is the aggregate-state snapshot cache (ADR 0007/0009): a
// pure, re-derivable optimization that lets the runtime seed folding from a
// cached state at version N and fold only the tail (events > N) instead of the
// whole stream. Correctness always comes from the log, so a snapshot may be
// stale, absent, or wrong — the runtime just folds a longer tail.
//
// A Cache is workspace-scoped and stores opaque state bytes. In the
// client-owned-crypto model (ADR 0025) the caller seals state before Save if it
// contains sensitive data; the cache implementation does not encrypt.
package snapshot

import (
	"context"
	"time"
)

// Snapshot is a cached folded state at a point in a stream's history. State is
// the serialized state (es.StateCodec); Cache stores those bytes as-is. Callers
// must seal State if it contains sensitive data and purge snapshots when they
// destroy the corresponding key (ADR 0025).
type Snapshot struct {
	Version     uint64    // the stream version the state reflects
	FoldVersion uint32    // guards decider/upcaster/state-shape changes
	RecordedAt  time.Time // when the snapshot was taken (for bounded-staleness reads)
	State       []byte    // serialized aggregate state (plaintext to the runtime)
}

// Cache stores one snapshot per stream, keyed by canonical stream id within a
// workspace. Save is best-effort from the runtime's perspective (a failure
// only costs a longer fold next time); Load returns ok=false when absent.
type Cache interface {
	Load(ctx context.Context, streamID string) (Snapshot, bool, error)
	Save(ctx context.Context, streamID string, snap Snapshot) error
}
