// Package snapshot is the aggregate-state snapshot cache (ADR 0007/0009): a
// pure, re-derivable optimization that lets the runtime seed folding from a
// cached state at version N and fold only the tail (events > N) instead of the
// whole stream. Correctness always comes from the log, so a snapshot may be
// stale, absent, or wrong — the runtime just folds a longer tail.
//
// A Cache is workspace-scoped. In the zero-knowledge deployment the NATS KV
// implementation (snapshot/natskv) encrypts the state blob client-side, so KV
// holds only ciphertext.
package snapshot

import (
	"context"
	"time"
)

// Snapshot is a cached folded state at a point in a stream's history. State is
// the plaintext serialized state (es.StateCodec); the Cache implementation is
// responsible for encrypting it at rest.
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
