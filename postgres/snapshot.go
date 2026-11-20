package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/laenenai/es-lite/snapshot"
)

// SnapshotCache is the Postgres snapshot.Cache (ADR 0042 §3.2): one row per
// (workspace, stream) in the snapshots table (migration 0013), every
// operation inside runWorkspace so the table's workspace-only RLS policy —
// ENABLE + FORCE, no bypass arm — applies, exactly as it does for the feed
// tables.
//
// The workspace is bound at construction, because the Cache interface
// carries no workspace parameter: an implementation is workspace-scoped by
// construction (ADR 0042 §1). State is opaque — stored and returned
// verbatim, never decoded, never encrypted; sealing is the caller's codec's
// job (ADR 0025), and the table is trusted infrastructure at the same level
// as the events table: same database, same RLS, same grant discipline (ADR
// 0042 §3.3).
type SnapshotCache struct {
	store     *Store
	workspace string
}

var _ snapshot.Cache = (*SnapshotCache)(nil)

// NewSnapshotCache returns a snapshot.Cache over s, scoped to workspace for
// its whole lifetime.
func NewSnapshotCache(s *Store, workspace string) *SnapshotCache {
	return &SnapshotCache{store: s, workspace: workspace}
}

// Load returns the stored snapshot for streamID, or ok=false when the stream
// has none. The runtime treats every error the same as absence — a failed
// load falls through to a full fold — but the error is still returned so a
// caller that wants to see a missing table can.
func (c *SnapshotCache) Load(ctx context.Context, streamID string) (snapshot.Snapshot, bool, error) {
	var (
		snap        snapshot.Snapshot
		version     int64
		foldVersion int64
	)
	err := c.store.runWorkspace(ctx, c.workspace, func(tx DBTX) error {
		return tx.QueryRow(ctx,
			`SELECT version, fold_version, recorded_at, state FROM snapshots
			 WHERE workspace_id = $1 AND stream_id = $2`,
			c.workspace, streamID,
		).Scan(&version, &foldVersion, &snap.RecordedAt, &snap.State)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return snapshot.Snapshot{}, false, nil
	}
	if err != nil {
		return snapshot.Snapshot{}, false, fmt.Errorf("postgres: snapshots: load: %w", err)
	}
	snap.Version = uint64(version)
	snap.FoldVersion = uint32(foldVersion)
	return snap, true, nil
}

// Save writes one snapshot, last-writer-wins: the upsert has no monotonic
// guard on purpose (ADR 0042 §3.2). Per-stream appends are already
// serialized by expected-version conflicts, so synchronous writes cannot
// reorder; if two replicas' writes do arrive out of order, the older row
// costs one load at most n extra folded events and the next gap boundary
// rewrites it. A guard would buy that back only by permanently wedging a
// stream after a log reset, where the stale row could never be replaced
// from a lower version. recorded_at is the runtime's Snapshot.RecordedAt in
// UTC — the value LoadStale measures staleness against — never the
// database's clock.
func (c *SnapshotCache) Save(ctx context.Context, streamID string, snap snapshot.Snapshot) error {
	err := c.store.runWorkspace(ctx, c.workspace, func(tx DBTX) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO snapshots (workspace_id, stream_id, version, fold_version, recorded_at, state)
			 VALUES ($1, $2, $3, $4, $5, $6)
			 ON CONFLICT (workspace_id, stream_id) DO UPDATE SET
			     version = excluded.version,
			     fold_version = excluded.fold_version,
			     recorded_at = excluded.recorded_at,
			     state = excluded.state`,
			c.workspace, streamID, int64(snap.Version), int64(snap.FoldVersion), snap.RecordedAt.UTC(), snap.State,
		)
		return err
	})
	if err != nil {
		return fmt.Errorf("postgres: snapshots: save: %w", err)
	}
	return nil
}

// Purge deletes every snapshot this cache's workspace holds: the erasure
// surface ADR 0025 §4 makes the caller's obligation and §3.2 of ADR 0042
// gives a method. It is best-effort and application-coordinated — a command
// in flight, or one that starts immediately after, can re-insert a row — so
// the shred procedure must quiesce the workspace first (or destroy the
// state-sealing key, which makes any racing re-insert inert). It is not on
// snapshot.Cache, so the interface stays two methods and the shred
// procedure holds the concrete adapter.
func (c *SnapshotCache) Purge(ctx context.Context) error {
	err := c.store.runWorkspace(ctx, c.workspace, func(tx DBTX) error {
		_, err := tx.Exec(ctx, `DELETE FROM snapshots WHERE workspace_id = $1`, c.workspace)
		return err
	})
	if err != nil {
		return fmt.Errorf("postgres: snapshots: purge: %w", err)
	}
	return nil
}
