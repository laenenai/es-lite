package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/feed"
)

var (
	_ feed.Log             = (*Store)(nil)
	_ feed.Logs            = (*Logs)(nil)
	_ feed.CheckpointStore = (*CheckpointStore)(nil)
	_ feed.LeaseStore      = (*LeaseStore)(nil)
)

// Head returns the current log head: the highest global_position in the
// database, or 0 when it holds no events. It is the position a live feed
// resumes from and what Status compares a checkpoint against.
func (s *Store) Head(ctx context.Context) (uint64, error) {
	var head uint64
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(global_position), 0) FROM events`,
	).Scan(&head)
	if err != nil {
		return 0, fmt.Errorf("sqlite: head: %w", err)
	}
	return head, nil
}

// ReadAllBounded is the feed's paged read: events after from, in global
// order, filtered server-side to types, and bounded by a payload-byte budget
// over the bytes as stored. An empty types set returns no events at all.
// The page holds at most limit events (limit <= 0 means 100). It stops
// before an event whose payload would push the running total past maxBytes
// and reports truncated=true so the caller continues immediately; the first
// event is always included even alone over budget, so an oversized event is
// never silently skipped. maxBytes <= 0 disables the budget.
//
// The budget counts the opaque payload bytes this store returns (ADR 0025):
// for a sealed workspace those are the stored ciphertext bytes. Like the
// Postgres adapter it is deliberately not on es.Store.
func (s *Store) ReadAllBounded(ctx context.Context, from uint64, limit, maxBytes int, types []string) ([]es.Envelope, bool, error) {
	if len(types) == 0 {
		return nil, false, nil
	}
	if limit <= 0 {
		limit = 100
	}

	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(types)), ",")
	q := `SELECT ` + envelopeCols + ` FROM events
	      WHERE global_position > ? AND type_url IN (` + placeholders + `)
	      ORDER BY global_position ASC LIMIT ?`
	args := make([]any, 0, len(types)+2)
	args = append(args, from)
	for _, t := range types {
		args = append(args, t)
	}
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, false, fmt.Errorf("sqlite: read feed: %w", err)
	}
	defer rows.Close()

	var (
		out       []es.Envelope
		total     int
		truncated bool
	)
	for rows.Next() {
		e, err := scanEnvelope(rows)
		if err != nil {
			return nil, false, fmt.Errorf("sqlite: read feed: %w", err)
		}
		// The first row is carried whatever its size; after that, stop
		// before a row that would exceed the budget. The stopping row was
		// read, so more matching rows remain for the next call.
		if len(out) > 0 && maxBytes > 0 && total+len(e.Payload) > maxBytes {
			truncated = true
			break
		}
		total += len(e.Payload)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("sqlite: read feed: %w", err)
	}
	return out, truncated, nil
}

// CheckpointStore persists feed delivery cursors in SQLite. Keys are
// (scope, projection, instance); scope is carried for parity with
// multi-tenant backends, while one SQLite database is one workspace.
type CheckpointStore struct {
	store *Store
}

// NewCheckpointStore returns a CheckpointStore backed by s.
func NewCheckpointStore(s *Store) *CheckpointStore { return &CheckpointStore{store: s} }

// Load returns the stored position, or 0 when no checkpoint exists.
func (c *CheckpointStore) Load(ctx context.Context, scope, projection, instance string) (uint64, error) {
	var pos uint64
	err := c.store.db.QueryRowContext(ctx,
		`SELECT position FROM feed_checkpoints
		 WHERE scope = ? AND projection = ? AND instance = ?`,
		scope, projection, instance,
	).Scan(&pos)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("sqlite: checkpoints: load: %w", err)
	}
	return pos, nil
}

// Save advances a checkpoint monotonically: a lower position never rewinds a
// higher one. The timestamp still records the write.
func (c *CheckpointStore) Save(ctx context.Context, scope, projection, instance string, position uint64) error {
	_, err := c.store.db.ExecContext(ctx,
		`INSERT INTO feed_checkpoints (scope, projection, instance, position, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(scope, projection, instance) DO UPDATE SET
		     position = MAX(feed_checkpoints.position, excluded.position),
		     updated_at = excluded.updated_at`,
		scope, projection, instance, position, formatTS(time.Now()),
	)
	if err != nil {
		return fmt.Errorf("sqlite: checkpoints: save: %w", err)
	}
	return nil
}

// Reset overwrites a checkpoint, backwards included. A reset is a deliberate
// replay decision, so unlike Save it does not keep the maximum.
func (c *CheckpointStore) Reset(ctx context.Context, scope, projection, instance string, position uint64) error {
	_, err := c.store.db.ExecContext(ctx,
		`INSERT INTO feed_checkpoints (scope, projection, instance, position, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(scope, projection, instance) DO UPDATE SET
		     position = excluded.position,
		     updated_at = excluded.updated_at`,
		scope, projection, instance, position, formatTS(time.Now()),
	)
	if err != nil {
		return fmt.Errorf("sqlite: checkpoints: reset: %w", err)
	}
	return nil
}

// Delete removes exactly one (scope, projection, instance) row.
func (c *CheckpointStore) Delete(ctx context.Context, scope, projection, instance string) error {
	_, err := c.store.db.ExecContext(ctx,
		`DELETE FROM feed_checkpoints
		 WHERE scope = ? AND projection = ? AND instance = ?`,
		scope, projection, instance,
	)
	if err != nil {
		return fmt.Errorf("sqlite: checkpoints: delete: %w", err)
	}
	return nil
}

// Checkpoint returns the stored position and when it was last written, or
// found=false when the instance has no row.
func (c *CheckpointStore) Checkpoint(ctx context.Context, scope, projection, instance string) (feed.Checkpoint, bool, error) {
	var (
		cp        feed.Checkpoint
		updatedAt string
	)
	err := c.store.db.QueryRowContext(ctx,
		`SELECT position, updated_at FROM feed_checkpoints
		 WHERE scope = ? AND projection = ? AND instance = ?`,
		scope, projection, instance,
	).Scan(&cp.Position, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return feed.Checkpoint{}, false, nil
	}
	if err != nil {
		return feed.Checkpoint{}, false, fmt.Errorf("sqlite: checkpoints: read: %w", err)
	}
	cp.UpdatedAt, err = parseTS(updatedAt)
	if err != nil {
		return feed.Checkpoint{}, false, fmt.Errorf("sqlite: checkpoints: parse updated_at %q: %w", updatedAt, err)
	}
	return cp, true, nil
}

// LeaseStore fences one active feed subscriber per key in SQLite, mirroring
// the Postgres adapter: acquire wins when the key is free, expired, or
// already held by the same owner; renew extends only for the owner; holder
// reports only unexpired leases.
type LeaseStore struct {
	store *Store
}

// NewLeaseStore returns a LeaseStore backed by s.
func NewLeaseStore(s *Store) *LeaseStore { return &LeaseStore{store: s} }

// Acquire takes the lease for key, or fails when another live owner holds
// it. Re-acquiring with the same owner succeeds and records the consumer
// named on this call. Expiry is stored as an absolute Unix-nanosecond
// instant.
func (l *LeaseStore) Acquire(ctx context.Context, key, owner, consumer string, ttl time.Duration) (bool, error) {
	now := time.Now()
	res, err := l.store.db.ExecContext(ctx,
		`INSERT INTO feed_leases (key, owner, consumer, expires_at_ns)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(key) DO UPDATE SET
		     owner = excluded.owner,
		     consumer = excluded.consumer,
		     expires_at_ns = excluded.expires_at_ns
		 WHERE feed_leases.owner = excluded.owner
		    OR feed_leases.expires_at_ns <= ?`,
		key, owner, consumer, now.Add(ttl).UnixNano(), now.UnixNano(),
	)
	if err != nil {
		return false, fmt.Errorf("sqlite: leases: acquire: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("sqlite: leases: acquire: %w", err)
	}
	return n == 1, nil
}

// Renew extends the lease only for its current owner. Like the Postgres
// adapter it does not check whether the row has already expired: the upsert
// owner guard at Acquire is what decides a takeover.
func (l *LeaseStore) Renew(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	res, err := l.store.db.ExecContext(ctx,
		`UPDATE feed_leases SET expires_at_ns = ?
		 WHERE key = ? AND owner = ?`,
		time.Now().Add(ttl).UnixNano(), key, owner,
	)
	if err != nil {
		return false, fmt.Errorf("sqlite: leases: renew: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("sqlite: leases: renew: %w", err)
	}
	return n == 1, nil
}

// Release drops the lease when owner still holds it. Releasing a lease
// nobody holds, or one another owner took over, is not an error.
func (l *LeaseStore) Release(ctx context.Context, key, owner string) error {
	if _, err := l.store.db.ExecContext(ctx,
		`DELETE FROM feed_leases WHERE key = ? AND owner = ?`,
		key, owner,
	); err != nil {
		return fmt.Errorf("sqlite: leases: release: %w", err)
	}
	return nil
}

// Holder reports the live holder of key, or found=false when nobody holds
// it. An expired row is not a holder: Acquire treats it as free, so this
// must too.
func (l *LeaseStore) Holder(ctx context.Context, key string) (feed.Lease, bool, error) {
	var (
		lease       feed.Lease
		expiresAtNS int64
	)
	err := l.store.db.QueryRowContext(ctx,
		`SELECT owner, consumer, expires_at_ns FROM feed_leases
		 WHERE key = ? AND expires_at_ns > ?`,
		key, time.Now().UnixNano(),
	).Scan(&lease.Owner, &lease.Consumer, &expiresAtNS)
	if errors.Is(err, sql.ErrNoRows) {
		return feed.Lease{}, false, nil
	}
	if err != nil {
		return feed.Lease{}, false, fmt.Errorf("sqlite: leases: holder: %w", err)
	}
	lease.ExpiresAt = time.Unix(0, expiresAtNS).UTC()
	return lease, true, nil
}

// Logs opens the feed Log. SQLite is single-workspace: scope is carried for
// parity with multi-tenant backends and for named-tenant callers, but one
// database is one workspace. This adapter does not imply row-level security
// or any other tenant isolation SQLite cannot enforce; callers must not
// treat distinct scopes as isolated.
type Logs struct {
	store *Store
}

// NewLogs returns a Logs over s.
func NewLogs(s *Store) *Logs { return &Logs{store: s} }

// Open implements feed.Logs. Every scope opens the same single-workspace log.
func (l *Logs) Open(_ context.Context, _ string) (feed.Log, error) { return l.store, nil }
