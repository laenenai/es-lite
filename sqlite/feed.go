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
	_ feed.DeadLetterStore = (*DeadLetterStore)(nil)
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

// RawHead returns the raw committed maximum with no settle bound: the
// highest global_position among committed events, or 0 when there are
// none. SQLite has no horizon fence (spec §5.1 is Postgres-only — one
// connection serializes writers, so a reader can never observe a position
// whose insert committed later), so RawHead equals Head; it exists because
// the feed's fence-aware resume clamp asks every Log for the unbounded
// view. Stall detection reads its filtered counterpart, MatchingRawHead.
func (s *Store) RawHead(ctx context.Context) (uint64, error) {
	var head uint64
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(global_position), 0) FROM events`,
	).Scan(&head)
	if err != nil {
		return 0, fmt.Errorf("sqlite: raw head: %w", err)
	}
	return head, nil
}

// MatchingHead returns the highest global_position among events of the
// given types, or 0 when none match — an empty types set matches nothing.
// It is the subscription's own head: what Status reports and measures
// lag against. On SQLite it is a simple filtered MAX.
func (s *Store) MatchingHead(ctx context.Context, types []string) (uint64, error) {
	if len(types) == 0 {
		return 0, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(types)), ",")
	q := `SELECT COALESCE(MAX(global_position), 0) FROM events
	      WHERE type_url IN (` + placeholders + `)`
	args := make([]any, 0, len(types))
	for _, t := range types {
		args = append(args, t)
	}
	var head uint64
	if err := s.db.QueryRowContext(ctx, q, args...).Scan(&head); err != nil {
		return 0, fmt.Errorf("sqlite: matching head: %w", err)
	}
	return head, nil
}

// MatchingRawHead returns the highest raw committed position among
// events of the given types, or 0 when none match — an empty types set
// matches nothing. It is the stall-detection view (spec §5.8): the
// subscription's own backlog with no settle bound. SQLite has no horizon
// fence (spec §5.1 is Postgres-only — one connection serializes
// writers, so a reader can never observe a position whose insert
// committed later), so the raw and settled views cannot diverge and
// MatchingRawHead runs MatchingHead's exact query — the only difference
// in its body is the error label, which names this method. Stall
// detection is a liveness signal, not a correctness boundary — a torn
// read can only mis-time a stall report, never corrupt data — and on
// this backend no torn read is even possible. Deliberately not on
// es.Store.
func (s *Store) MatchingRawHead(ctx context.Context, types []string) (uint64, error) {
	if len(types) == 0 {
		return 0, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(types)), ",")
	q := `SELECT COALESCE(MAX(global_position), 0) FROM events
	      WHERE type_url IN (` + placeholders + `)`
	args := make([]any, 0, len(types))
	for _, t := range types {
		args = append(args, t)
	}
	var head uint64
	if err := s.db.QueryRowContext(ctx, q, args...).Scan(&head); err != nil {
		return 0, fmt.Errorf("sqlite: matching raw head: %w", err)
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
// The page is fetched in two steps so a payload over the store's cap
// (WithMaxPayloadBytes) is never materialized (production review): step 1
// selects positions, type URLs and length(payload) — sizes only, never the
// bytes — under the filter and applies the count/budget/cap selection;
// step 2 fetches full rows for the chosen positions alone. A first row over
// the cap is a poison event: the page refuses to carry it and returns
// *feed.EventTooLargeError naming position, type URL, size and limit —
// never the payload. A later row over the cap stops the page before it
// with truncated=true, so the next page makes it the first row and
// surfaces the error.
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

	// Step 1: positions, type URLs and payload sizes under the filter —
	// never the payload bytes. length() counts bytes for the BLOB payloads
	// this store writes, so an over-cap row costs one integer, not its
	// payload.
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(types)), ",")
	q := `SELECT global_position, type_url, length(payload) FROM events
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
		positions []uint64
		total     int
		truncated bool
	)
	for rows.Next() {
		var (
			pos     uint64
			typeURL string
			size    int
		)
		if err := rows.Scan(&pos, &typeURL, &size); err != nil {
			return nil, false, fmt.Errorf("sqlite: read feed: %w", err)
		}
		if len(positions) == 0 && s.maxPayloadBytes > 0 && size > s.maxPayloadBytes {
			// The first row of the page is over the store's cap: a poison
			// event. Refuse to materialize it; the typed error names it
			// without the payload bytes ever leaving the database.
			return nil, false, &feed.EventTooLargeError{
				Position: pos,
				TypeURL:  typeURL,
				Size:     size,
				Limit:    s.maxPayloadBytes,
			}
		}
		// A later row over the cap stops the page before it — the next
		// page makes it the first row and surfaces the error — and so does
		// a row that would push the total past the budget. The stopping
		// row was seen, so truncated is certain: more rows remain for the
		// next call.
		if len(positions) > 0 &&
			((s.maxPayloadBytes > 0 && size > s.maxPayloadBytes) ||
				(maxBytes > 0 && total+size > maxBytes)) {
			truncated = true
			break
		}
		total += size
		positions = append(positions, pos)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("sqlite: read feed: %w", err)
	}
	if len(positions) == 0 {
		return nil, false, nil
	}
	// Step 2: fetch full rows for the chosen positions only. This store
	// runs on a single connection, so the page is a stable snapshot by
	// construction; closing step 1's rows first is what frees the one
	// connection for the fetch.
	rows.Close()
	posPlaceholders := strings.TrimSuffix(strings.Repeat("?,", len(positions)), ",")
	fetch := `SELECT ` + envelopeCols + ` FROM events
	      WHERE global_position IN (` + posPlaceholders + `)
	      ORDER BY global_position ASC`
	fetchArgs := make([]any, 0, len(positions))
	for _, p := range positions {
		fetchArgs = append(fetchArgs, int64(p))
	}
	out, err := s.queryEnvelopes(ctx, fetch, fetchArgs...)
	if err != nil {
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
// higher one. definition is the checkpoint's new definition fingerprint
// when non-empty; an empty definition preserves the stored hash, so an ack
// that carries no identity cannot clear what a previous one recorded. The
// timestamp still records the write.
func (c *CheckpointStore) Save(ctx context.Context, scope, projection, instance, definition string, position uint64) error {
	_, err := c.store.db.ExecContext(ctx,
		`INSERT INTO feed_checkpoints (scope, projection, instance, position, definition_hash, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(scope, projection, instance) DO UPDATE SET
		     position = MAX(feed_checkpoints.position, excluded.position),
		     definition_hash = CASE WHEN excluded.definition_hash = '' THEN feed_checkpoints.definition_hash ELSE excluded.definition_hash END,
		     updated_at = excluded.updated_at`,
		scope, projection, instance, position, definition, formatTS(time.Now()),
	)
	if err != nil {
		return fmt.Errorf("sqlite: checkpoints: save: %w", err)
	}
	return nil
}

// Reset overwrites a checkpoint, backwards included. A reset is a deliberate
// replay decision, so unlike Save it does not keep the maximum; an empty
// definition still preserves the stored fingerprint — only a reset under a
// new definition re-identifies the checkpoint.
func (c *CheckpointStore) Reset(ctx context.Context, scope, projection, instance, definition string, position uint64) error {
	_, err := c.store.db.ExecContext(ctx,
		`INSERT INTO feed_checkpoints (scope, projection, instance, position, definition_hash, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(scope, projection, instance) DO UPDATE SET
		     position = excluded.position,
		     definition_hash = CASE WHEN excluded.definition_hash = '' THEN feed_checkpoints.definition_hash ELSE excluded.definition_hash END,
		     updated_at = excluded.updated_at`,
		scope, projection, instance, position, definition, formatTS(time.Now()),
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

// Checkpoint returns the stored position, its definition fingerprint and
// when it was last written, or found=false when the instance has no row.
func (c *CheckpointStore) Checkpoint(ctx context.Context, scope, projection, instance string) (feed.Checkpoint, bool, error) {
	var (
		cp        feed.Checkpoint
		updatedAt string
	)
	err := c.store.db.QueryRowContext(ctx,
		`SELECT position, definition_hash, updated_at FROM feed_checkpoints
		 WHERE scope = ? AND projection = ? AND instance = ?`,
		scope, projection, instance,
	).Scan(&cp.Position, &cp.DefinitionHash, &updatedAt)
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

// DeadLetterStore records Skip's audit rows in SQLite: one row per
// deliberately skipped position, keyed with the subscription and the
// position — a repeat skip of the same position replaces the row. The row
// is a pointer, not a copy (spec §5.3): the event itself stays in the
// immutable log and the row only names it, so nothing is duplicated into a
// second retention surface. Triage reads this table directly over SQL.
type DeadLetterStore struct {
	store *Store
}

// NewDeadLetterStore returns a DeadLetterStore backed by s.
func NewDeadLetterStore(s *Store) *DeadLetterStore { return &DeadLetterStore{store: s} }

// Record inserts one dead-letter row, replacing any earlier row for the
// same (scope, projection, instance, position). Skip delivers the pointer
// fields — key, position, reason, actor — and cannot know the skipped
// event's type or size: a skip request carries no definition to authorize
// a filtered log read, and the log port never materializes an over-cap
// row. This store shares the database with the log, so when the incoming
// DeadLetter carries no type it names the event from the events row at
// dl.Position — metadata only: type_url and length(payload), never the
// payload bytes. A position with no event row records the empty type and
// zero size; Skip does not require the position to exist. A zero
// CreatedAt is stamped here, like feed_checkpoints.updated_at.
func (d *DeadLetterStore) Record(ctx context.Context, dl feed.DeadLetter) error {
	if dl.TypeURL == "" {
		// The core cannot know the skipped event's type or size, so name
		// the event from the log this store shares a database with —
		// metadata only: type_url and length(payload), never the payload
		// bytes (the poison row's over-cap payload stays unscanned). A
		// position with no event row names nothing; the pointer still
		// lands, because Skip does not require the position to exist.
		var (
			typeURL string
			size    int
		)
		err := d.store.db.QueryRowContext(ctx,
			`SELECT type_url, length(payload) FROM events WHERE global_position = ?`,
			dl.Position,
		).Scan(&typeURL, &size)
		if errors.Is(err, sql.ErrNoRows) {
			err = nil
		}
		if err != nil {
			return fmt.Errorf("sqlite: dead letters: name the event at %d: %w", dl.Position, err)
		}
		dl.TypeURL, dl.SizeBytes = typeURL, size
	}
	createdAt := dl.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	_, err := d.store.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO feed_dead_letters
		     (scope, projection, instance, position, type_url, size_bytes, reason, actor, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		dl.Scope, dl.Projection, dl.Instance, dl.Position,
		dl.TypeURL, dl.SizeBytes, dl.Reason, dl.Actor, formatTS(createdAt),
	)
	if err != nil {
		return fmt.Errorf("sqlite: dead letters: record: %w", err)
	}
	return nil
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

// leaseKey composes the TEXT key this adapter stores for the subscription
// triple. The feed core's validateKey refuses control bytes in every part,
// so the NUL separator can never occur inside a part and distinct triples
// cannot collide.
func leaseKey(scope, projection, instance string) string {
	return scope + "\x00" + projection + "\x00" + instance
}

// Acquire takes the lease for the (scope, projection, instance) key, or fails
// when another live owner holds it. Re-acquiring with the same owner succeeds
// and records the consumer named on this call. Expiry is stored as an
// absolute Unix-nanosecond instant.
func (l *LeaseStore) Acquire(ctx context.Context, scope, projection, instance, owner, consumer string, ttl time.Duration) (bool, error) {
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
		leaseKey(scope, projection, instance), owner, consumer, now.Add(ttl).UnixNano(), now.UnixNano(),
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
func (l *LeaseStore) Renew(ctx context.Context, scope, projection, instance, owner string, ttl time.Duration) (bool, error) {
	res, err := l.store.db.ExecContext(ctx,
		`UPDATE feed_leases SET expires_at_ns = ?
		 WHERE key = ? AND owner = ?`,
		time.Now().Add(ttl).UnixNano(), leaseKey(scope, projection, instance), owner,
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
func (l *LeaseStore) Release(ctx context.Context, scope, projection, instance, owner string) error {
	if _, err := l.store.db.ExecContext(ctx,
		`DELETE FROM feed_leases WHERE key = ? AND owner = ?`,
		leaseKey(scope, projection, instance), owner,
	); err != nil {
		return fmt.Errorf("sqlite: leases: release: %w", err)
	}
	return nil
}

// Holder reports the live holder of the (scope, projection, instance) key,
// or found=false when nobody holds it. An expired row is not a holder:
// Acquire treats it as free, so this must too.
func (l *LeaseStore) Holder(ctx context.Context, scope, projection, instance string) (feed.Lease, bool, error) {
	var (
		lease       feed.Lease
		expiresAtNS int64
	)
	err := l.store.db.QueryRowContext(ctx,
		`SELECT owner, consumer, expires_at_ns FROM feed_leases
		 WHERE key = ? AND expires_at_ns > ?`,
		leaseKey(scope, projection, instance), time.Now().UnixNano(),
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
