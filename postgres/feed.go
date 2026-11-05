package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/laenenai/es-lite/feed"
)

// The workspace-scoped feed adapters (spec §5.6): CheckpointStore,
// LeaseStore, DeadLetterStore and the Logs opener. Every operation runs
// in one short transaction with app.workspace_id bound to the scope
// (setWorkspace), so the feed tables' RLS policies apply — they carry no
// bypass arm — and every timestamp is the database's own clock
// (clock_timestamp() in SQL), never the caller's, so feed replicas do
// not trust each other's clocks. The semantics mirror sqlite/feed.go
// exactly: monotonic checkpoint Save, rewinding Reset, empty-definition
// preserve, owner-guarded lease acquire with expiry takeover, renew only
// by owner, holder only while unexpired.

var (
	_ feed.Log             = (*Workspace)(nil)
	_ feed.Logs            = (*Logs)(nil)
	_ feed.CheckpointStore = (*CheckpointStore)(nil)
	_ feed.LeaseStore      = (*LeaseStore)(nil)
	_ feed.DeadLetterStore = (*DeadLetterStore)(nil)
)

// runWorkspace runs fn in one short transaction with app.workspace_id
// bound to ws (is_local=true) — the same pinning the es.Store operations
// use. The feed tables' policies see the workspace, and the setting
// cannot outlive the transaction. fn's error is returned unwrapped so
// callers keep errors.Is checks (pgx.ErrNoRows) intact. The transaction
// commits when fn returns nil; for the single-statement reads here a
// read-only commit is a no-op that just releases the snapshot.
func (s *Store) runWorkspace(ctx context.Context, ws string, fn func(tx DBTX) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: feed: begin: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := setWorkspace(ctx, tx, ws); err != nil {
		return fmt.Errorf("postgres: feed: set workspace: %w", err)
	}
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: feed: commit: %w", err)
	}
	return nil
}

// Logs opens the per-scope feed Log (feed.Logs). The feed's scope maps
// 1:1 to a workspace id — the platform's transport authorizes the scope
// upstream — and Open hands back the workspace-pinned *Workspace, which
// implements feed.Log: fenced ReadAllBounded pages (spec §5.1), the
// settled Head and MatchingHead, and the unbounded RawHead.
type Logs struct {
	store *Store
}

// NewLogs returns a Logs over s.
func NewLogs(s *Store) *Logs { return &Logs{store: s} }

// Open implements feed.Logs: scope is the workspace id.
func (l *Logs) Open(_ context.Context, scope string) (feed.Log, error) {
	return l.store.WorkspaceStore(scope), nil
}

// CheckpointStore persists feed delivery cursors in Postgres. The key is
// the (scope, projection, instance) triple; scope is the workspace id,
// so every row is workspace-scoped under a policy with no bypass arm,
// and updated_at is the database's clock at the write (clock_timestamp()
// in SQL), not the writer's.
type CheckpointStore struct {
	store *Store
}

// NewCheckpointStore returns a CheckpointStore backed by s.
func NewCheckpointStore(s *Store) *CheckpointStore { return &CheckpointStore{store: s} }

// Load returns the stored position, or 0 when no checkpoint exists.
func (c *CheckpointStore) Load(ctx context.Context, scope, projection, instance string) (uint64, error) {
	var pos int64
	err := c.store.runWorkspace(ctx, scope, func(tx DBTX) error {
		return tx.QueryRow(ctx,
			`SELECT position FROM feed_checkpoints
			 WHERE workspace_id = $1 AND projection = $2 AND instance = $3`,
			scope, projection, instance,
		).Scan(&pos)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("postgres: checkpoints: load: %w", err)
	}
	return uint64(pos), nil
}

// Save advances a checkpoint monotonically: a lower position never
// rewinds a higher one (GREATEST in SQL, the SQLite adapter's MAX).
// definition is the checkpoint's new definition fingerprint when
// non-empty; an empty definition preserves the stored hash, so an ack
// that carries no identity cannot clear what a previous one recorded.
// updated_at is clock_timestamp(): the DB clock, not the writer's.
func (c *CheckpointStore) Save(ctx context.Context, scope, projection, instance, definition string, position uint64) error {
	err := c.store.runWorkspace(ctx, scope, func(tx DBTX) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO feed_checkpoints (workspace_id, projection, instance, position, definition_hash, updated_at)
			 VALUES ($1, $2, $3, $4, $5, clock_timestamp())
			 ON CONFLICT (workspace_id, projection, instance) DO UPDATE SET
			     position = GREATEST(feed_checkpoints.position, excluded.position),
			     definition_hash = CASE WHEN excluded.definition_hash = '' THEN feed_checkpoints.definition_hash ELSE excluded.definition_hash END,
			     updated_at = clock_timestamp()`,
			scope, projection, instance, int64(position), definition,
		)
		return err
	})
	if err != nil {
		return fmt.Errorf("postgres: checkpoints: save: %w", err)
	}
	return nil
}

// Reset overwrites a checkpoint, backwards included. A reset is a
// deliberate replay decision, so unlike Save it does not keep the
// maximum; an empty definition still preserves the stored fingerprint —
// only a reset under a new definition re-identifies the checkpoint.
func (c *CheckpointStore) Reset(ctx context.Context, scope, projection, instance, definition string, position uint64) error {
	err := c.store.runWorkspace(ctx, scope, func(tx DBTX) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO feed_checkpoints (workspace_id, projection, instance, position, definition_hash, updated_at)
			 VALUES ($1, $2, $3, $4, $5, clock_timestamp())
			 ON CONFLICT (workspace_id, projection, instance) DO UPDATE SET
			     position = excluded.position,
			     definition_hash = CASE WHEN excluded.definition_hash = '' THEN feed_checkpoints.definition_hash ELSE excluded.definition_hash END,
			     updated_at = clock_timestamp()`,
			scope, projection, instance, int64(position), definition,
		)
		return err
	})
	if err != nil {
		return fmt.Errorf("postgres: checkpoints: reset: %w", err)
	}
	return nil
}

// Delete removes exactly one (scope, projection, instance) row.
func (c *CheckpointStore) Delete(ctx context.Context, scope, projection, instance string) error {
	err := c.store.runWorkspace(ctx, scope, func(tx DBTX) error {
		_, err := tx.Exec(ctx,
			`DELETE FROM feed_checkpoints
			 WHERE workspace_id = $1 AND projection = $2 AND instance = $3`,
			scope, projection, instance,
		)
		return err
	})
	if err != nil {
		return fmt.Errorf("postgres: checkpoints: delete: %w", err)
	}
	return nil
}

// Checkpoint returns the stored position, its definition fingerprint and
// when it was last written, or found=false when the instance has no
// row. UpdatedAt is the database's clock at the last write.
func (c *CheckpointStore) Checkpoint(ctx context.Context, scope, projection, instance string) (feed.Checkpoint, bool, error) {
	var cp feed.Checkpoint
	err := c.store.runWorkspace(ctx, scope, func(tx DBTX) error {
		return tx.QueryRow(ctx,
			`SELECT position, definition_hash, updated_at FROM feed_checkpoints
			 WHERE workspace_id = $1 AND projection = $2 AND instance = $3`,
			scope, projection, instance,
		).Scan(&cp.Position, &cp.DefinitionHash, &cp.UpdatedAt)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return feed.Checkpoint{}, false, nil
	}
	if err != nil {
		return feed.Checkpoint{}, false, fmt.Errorf("postgres: checkpoints: read: %w", err)
	}
	return cp, true, nil
}

// LeaseStore fences one active feed subscriber per key in Postgres,
// mirroring the SQLite adapter exactly: acquire wins when the key is
// free, expired, or already held by the same owner; renew extends only
// for the owner; holder reports only unexpired leases. The key is the
// three columns — no composed storage key (spec §5.4) — and expiry is
// absolute wall time computed from the database's clock in SQL, so a
// replica whose clock runs behind never extends a lease into extra life.
type LeaseStore struct {
	store *Store
}

// NewLeaseStore returns a LeaseStore backed by s.
func NewLeaseStore(s *Store) *LeaseStore { return &LeaseStore{store: s} }

// Acquire takes the lease for the (scope, projection, instance) key, or
// fails when another live owner holds it. One owner-guarded upsert: the
// conflict path overwrites only when the row is already this owner's or
// its expiry has passed — expires_at <= clock_timestamp(), the same
// boundary the SQLite adapter pins — so a refused acquire changes
// nothing and reports false through the zero rows affected.
// Re-acquiring with the same owner succeeds and records the consumer
// named on this call.
func (l *LeaseStore) Acquire(ctx context.Context, scope, projection, instance, owner, consumer string, ttl time.Duration) (bool, error) {
	var acquired bool
	err := l.store.runWorkspace(ctx, scope, func(tx DBTX) error {
		tag, err := tx.Exec(ctx,
			`INSERT INTO feed_leases (workspace_id, projection, instance, owner, consumer, expires_at)
			 VALUES ($1, $2, $3, $4, $5, clock_timestamp() + $6::bigint * interval '1 microsecond')
			 ON CONFLICT (workspace_id, projection, instance) DO UPDATE SET
			     owner = excluded.owner,
			     consumer = excluded.consumer,
			     expires_at = excluded.expires_at
			 WHERE feed_leases.owner = excluded.owner
			    OR feed_leases.expires_at <= clock_timestamp()`,
			scope, projection, instance, owner, consumer, ttl.Microseconds(),
		)
		if err != nil {
			return err
		}
		acquired = tag.RowsAffected() == 1
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("postgres: leases: acquire: %w", err)
	}
	return acquired, nil
}

// Renew extends the lease only for its current owner, from the
// database's clock. Like the SQLite adapter it does not check whether
// the row has already expired: the owner guard at Acquire is what
// decides a takeover.
func (l *LeaseStore) Renew(ctx context.Context, scope, projection, instance, owner string, ttl time.Duration) (bool, error) {
	var renewed bool
	err := l.store.runWorkspace(ctx, scope, func(tx DBTX) error {
		tag, err := tx.Exec(ctx,
			`UPDATE feed_leases SET expires_at = clock_timestamp() + $5::bigint * interval '1 microsecond'
			 WHERE workspace_id = $1 AND projection = $2 AND instance = $3 AND owner = $4`,
			scope, projection, instance, owner, ttl.Microseconds(),
		)
		if err != nil {
			return err
		}
		renewed = tag.RowsAffected() == 1
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("postgres: leases: renew: %w", err)
	}
	return renewed, nil
}

// Release drops the lease when owner still holds it. Releasing a lease
// nobody holds, or one another owner took over, is not an error.
func (l *LeaseStore) Release(ctx context.Context, scope, projection, instance, owner string) error {
	err := l.store.runWorkspace(ctx, scope, func(tx DBTX) error {
		_, err := tx.Exec(ctx,
			`DELETE FROM feed_leases
			 WHERE workspace_id = $1 AND projection = $2 AND instance = $3 AND owner = $4`,
			scope, projection, instance, owner,
		)
		return err
	})
	if err != nil {
		return fmt.Errorf("postgres: leases: release: %w", err)
	}
	return nil
}

// Holder reports the live holder of the (scope, projection, instance)
// key, or found=false when nobody holds it. An expired row is not a
// holder: Acquire treats it as free (expires_at <= clock_timestamp()),
// so this reports only rows strictly past their expiry — the same
// `<=` acquire / `>` holder boundary pair the SQLite adapter pins.
func (l *LeaseStore) Holder(ctx context.Context, scope, projection, instance string) (feed.Lease, bool, error) {
	var lease feed.Lease
	err := l.store.runWorkspace(ctx, scope, func(tx DBTX) error {
		return tx.QueryRow(ctx,
			`SELECT owner, consumer, expires_at FROM feed_leases
			 WHERE workspace_id = $1 AND projection = $2 AND instance = $3
			   AND expires_at > clock_timestamp()`,
			scope, projection, instance,
		).Scan(&lease.Owner, &lease.Consumer, &lease.ExpiresAt)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return feed.Lease{}, false, nil
	}
	if err != nil {
		return feed.Lease{}, false, fmt.Errorf("postgres: leases: holder: %w", err)
	}
	lease.ExpiresAt = lease.ExpiresAt.UTC()
	return lease, true, nil
}

// DeadLetterStore records Skip's audit rows in Postgres: one pointer row
// per deliberately skipped position (spec §5.3), keyed with the
// subscription and the position — a repeat skip of the same position
// replaces the row. The row is a pointer, not a copy: the event itself
// stays in the immutable log and the row only names it, so nothing is
// duplicated into a second retention surface. Triage reads this table
// directly over SQL.
type DeadLetterStore struct {
	store *Store
}

// NewDeadLetterStore returns a DeadLetterStore backed by s.
func NewDeadLetterStore(s *Store) *DeadLetterStore { return &DeadLetterStore{store: s} }

// Record inserts one dead-letter row, replacing any earlier row for the
// same (scope, projection, instance, position).
//
// The core passes the pointer fields — key, position, reason, actor —
// and cannot know the skipped event's type or size: a skip request
// carries no definition to authorize a filtered log read, and the log
// port never materializes an over-cap row. So when the incoming
// DeadLetter carries no type, this store names the event from the events
// row at dl.Position — metadata only: type_url and octet_length(payload),
// never the payload bytes, which an over-cap poison row must never
// surrender. The lookup runs inside the same workspace-pinned
// transaction, filtered by workspace both explicitly and through the
// events policy: a position belonging to another workspace names
// nothing, and a position with no event row at all names nothing too —
// Skip does not require the position to exist. Either way the pointer
// still lands with the empty type and zero size.
//
// created_at is clock_timestamp() in SQL — the database's clock, not
// dl.CreatedAt — so every replica's audit trail shares one monotone
// clock (spec §5.6). The SQLite adapter honors dl.CreatedAt; this
// divergence is deliberate, and pinned by the tests.
func (d *DeadLetterStore) Record(ctx context.Context, dl feed.DeadLetter) error {
	err := d.store.runWorkspace(ctx, dl.Scope, func(tx DBTX) error {
		if dl.TypeURL == "" {
			// Name the event from the log this store shares a database
			// with — metadata only, never the payload bytes. A position
			// with no event row in this workspace leaves the empty type
			// and zero size; the pointer still lands.
			var (
				typeURL string
				size    int
			)
			err := tx.QueryRow(ctx,
				`SELECT type_url, octet_length(payload) FROM events
				 WHERE workspace_id = $1 AND global_position = $2`,
				dl.Scope, int64(dl.Position),
			).Scan(&typeURL, &size)
			if errors.Is(err, pgx.ErrNoRows) {
				err = nil
			}
			if err != nil {
				return fmt.Errorf("postgres: dead letters: name the event at %d: %w", dl.Position, err)
			}
			dl.TypeURL, dl.SizeBytes = typeURL, size
		}
		_, err := tx.Exec(ctx,
			`INSERT INTO feed_dead_letters (
			     workspace_id, projection, instance, position, type_url, size_bytes, reason, actor, created_at
			 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, clock_timestamp())
			 ON CONFLICT (workspace_id, projection, instance, position) DO UPDATE SET
			     type_url = excluded.type_url,
			     size_bytes = excluded.size_bytes,
			     reason = excluded.reason,
			     actor = excluded.actor,
			     created_at = excluded.created_at`,
			dl.Scope, dl.Projection, dl.Instance, int64(dl.Position),
			dl.TypeURL, dl.SizeBytes, dl.Reason, dl.Actor,
		)
		return err
	})
	if err != nil {
		return fmt.Errorf("postgres: dead letters: record: %w", err)
	}
	return nil
}
