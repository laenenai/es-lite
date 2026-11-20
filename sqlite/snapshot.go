package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/laenenai/es-lite/snapshot"
)

// SnapshotCache is the SQLite snapshot.Cache (ADR 0042 §3.2): one row per
// stream in the snapshots table (migration 0011). One SQLite database is
// one workspace, so unlike the Postgres adapter there is no workspace
// parameter and no workspace column — the same single-workspace model the
// feed ports document in the package doc. State is opaque — stored and
// returned verbatim, never decoded, never encrypted; sealing is the
// caller's codec's job (ADR 0025), and the table is trusted infrastructure
// at the same level as the events table: same database, same grant
// discipline (ADR 0042 §3.3).
type SnapshotCache struct {
	store *Store
}

var _ snapshot.Cache = (*SnapshotCache)(nil)

// NewSnapshotCache returns a snapshot.Cache over s. The cache is scoped to
// the one workspace s's database holds for its whole lifetime.
func NewSnapshotCache(s *Store) *SnapshotCache {
	return &SnapshotCache{store: s}
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
		recordedAt  string
	)
	err := c.store.db.QueryRowContext(ctx,
		`SELECT version, fold_version, recorded_at, state FROM snapshots
		 WHERE stream_id = ?`,
		streamID,
	).Scan(&version, &foldVersion, &recordedAt, &snap.State)
	if errors.Is(err, sql.ErrNoRows) {
		return snapshot.Snapshot{}, false, nil
	}
	if err != nil {
		return snapshot.Snapshot{}, false, fmt.Errorf("sqlite: snapshots: load: %w", err)
	}
	if snap.RecordedAt, err = parseTS(recordedAt); err != nil {
		return snapshot.Snapshot{}, false, fmt.Errorf("sqlite: snapshots: load: parse recorded_at %q: %w", recordedAt, err)
	}
	// The int64→uint64 narrowing (SQLite integers are signed) wraps above
	// 2^63 — unreachable for real stream versions; the write path's
	// int64(snap.Version) is the same cast. fold_version's int64→uint32 is
	// lossless for the values a fold version takes.
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
// UTC, in the same fixed-width layout as events.recorded_at — the value
// LoadStale measures staleness against — never the database's clock. State
// must be non-nil: the column is NOT NULL, so a nil state fails the insert.
func (c *SnapshotCache) Save(ctx context.Context, streamID string, snap snapshot.Snapshot) error {
	// The int64 narrowing (SQLite integers are signed) wraps above 2^63 —
	// unreachable for real stream versions; feed.go's int64(position) is
	// the same cast. fold_version's uint32→int64 is lossless.
	_, err := c.store.db.ExecContext(ctx,
		`INSERT INTO snapshots (stream_id, version, fold_version, recorded_at, state)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(stream_id) DO UPDATE SET
		     version = excluded.version,
		     fold_version = excluded.fold_version,
		     recorded_at = excluded.recorded_at,
		     state = excluded.state`,
		streamID, int64(snap.Version), int64(snap.FoldVersion), formatTS(snap.RecordedAt), snap.State,
	)
	if err != nil {
		return fmt.Errorf("sqlite: snapshots: save: %w", err)
	}
	return nil
}

// Purge deletes every snapshot the database holds — the erasure surface
// ADR 0025 §4 makes the caller's obligation and §3.2 of ADR 0042 gives a
// method. With no workspace column, the database's snapshots are the
// cache's whole remit. It is best-effort and application-coordinated — a
// command in flight, or one that starts immediately after, can re-insert a
// row — so the shred procedure must quiesce the workspace first (or
// destroy the state-sealing key, which makes any racing re-insert inert).
// It is not on snapshot.Cache, so the interface stays two methods and the
// shred procedure holds the concrete adapter.
func (c *SnapshotCache) Purge(ctx context.Context) error {
	if _, err := c.store.db.ExecContext(ctx, `DELETE FROM snapshots`); err != nil {
		return fmt.Errorf("sqlite: snapshots: purge: %w", err)
	}
	return nil
}
