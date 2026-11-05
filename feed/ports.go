// Package feed is the transport-agnostic server core of the event feed: the
// checkpoint/lease/log ports and the subscribe loop that streams one
// projection instance's events to a caller-supplied Sink.
//
// It is deliberately storage- and transport-neutral. A Log returns opaque
// payload bytes (ADR 0025: the feed never unseals); the platform supplies a
// Postgres-backed Log, lease and checkpoint store, and maps a Connect stream
// to Sink. The platform also authorizes the tenant/projection and resolves the
// event type set before calling Subscribe: the core never consults a
// catalogue.
package feed

import (
	"context"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/projection"
)

// Checkpoint is one subscription's durable delivery cursor.
type Checkpoint struct {
	// Position is the highest global_position the consumer acknowledged.
	Position uint64
	// DefinitionHash is the fingerprint of the definition the position
	// was last acknowledged under (projection.Definition's Hash); empty
	// when none has been recorded. Subscribe compares it with the
	// request's definition and fails with ErrDefinitionChanged on a
	// mismatch, so a projection that gains a consumed type cannot
	// silently skip that type's history below the checkpoint.
	DefinitionHash string
	// UpdatedAt is when the checkpoint row was last written.
	UpdatedAt time.Time
}

// Lease is the current holder of a subscription key.
type Lease struct {
	// Owner identifies the stream that acquired the lease. It is opaque to
	// the consumer and unique per Subscribe call.
	Owner string
	// Consumer is the human/worker name that took the lease, for Status.
	Consumer string
	// ExpiresAt is when the lease stops fencing other subscribers unless it
	// is renewed first.
	ExpiresAt time.Time
}

// CheckpointStore persists per-(scope, projection, instance) delivery
// cursors together with the definition fingerprint the position was last
// acknowledged under. Implementations may enforce monotonic Save (the
// platform's Postgres store uses GREATEST); the core never assumes a
// rewind. Save and Reset take the fingerprint in definition: a non-empty
// value records it as the checkpoint's identity, an empty one preserves
// the stored hash — an ack that carries no identity, or a position-only
// advance like Skip, must not clear what a previous ack recorded.
type CheckpointStore interface {
	// Load returns the stored position, or (0, nil) when no checkpoint exists.
	Load(ctx context.Context, scope, projection, instance string) (uint64, error)
	// Save advances the checkpoint; definition is the new fingerprint
	// (empty preserves the stored one).
	Save(ctx context.Context, scope, projection, instance, definition string, position uint64) error
	// Reset overwrites the checkpoint, backwards included; definition is
	// the new fingerprint (empty preserves the stored one).
	Reset(ctx context.Context, scope, projection, instance, definition string, position uint64) error
	Delete(ctx context.Context, scope, projection, instance string) error
	// Checkpoint returns the stored position, fingerprint and last write
	// time, or found=false when the instance has no row.
	Checkpoint(ctx context.Context, scope, projection, instance string) (Checkpoint, bool, error)
}

// LeaseStore fences one active subscriber per subscription key (ADR 0002).
// The key is the (scope, projection, instance) triple, passed as separate
// parts so adapters can store them in their own shape — the Postgres adapter
// needs the scope column for row-level security. Adapters that join the
// parts into one storage key rely on validateKey refusing control bytes in
// every part. Acquire succeeds when the key is free, expired, or already
// owned by owner; Renew extends a lease only for its owner; Holder reports
// only unexpired leases.
type LeaseStore interface {
	Acquire(ctx context.Context, scope, projection, instance, owner, consumer string, ttl time.Duration) (bool, error)
	Renew(ctx context.Context, scope, projection, instance, owner string, ttl time.Duration) (bool, error)
	Release(ctx context.Context, scope, projection, instance, owner string) error
	Holder(ctx context.Context, scope, projection, instance string) (Lease, bool, error)
}

// Log is one scope's event log as the feed reads it. ReadAllBounded returns
// events after from in global order, filtered server-side to types, and
// bounded by a payload-byte budget over the bytes as stored: for a sealed
// workspace those are ciphertext bytes, so the feed never handles plaintext it
// does not need (ADR 0025). truncated reports that the budget stopped the
// page and more matching events remain, so the caller continues immediately
// instead of polling. The first event of a page is included even when it alone
// exceeds maxBytes, so an oversized event is visible to the caller.
//
// limit <= 0 and maxBytes <= 0 are adapter-defined defaults; the feed always
// passes positive values from its Config.
type Log interface {
	ReadAllBounded(ctx context.Context, from uint64, limit, maxBytes int, types []string) ([]es.Envelope, bool, error)
	Head(ctx context.Context) (uint64, error)
}

// Logs opens the Log for one scope (the platform's tenant/workspace).
type Logs interface {
	Open(ctx context.Context, scope string) (Log, error)
}

// Waiter is an optional wakeup source. Subscribe calls Wait after a short page
// instead of sleeping; Wait returns when new events may exist, after timeout,
// or when ctx ends. A normal timeout is a nil return: only ctx ending or a real
// failure returns an error, which stops the stream. A nil Waiter means polling.
type Waiter interface {
	Wait(ctx context.Context, timeout time.Duration) error
}

// Clock supplies wall time where the server needs it. Tests inject a fixed
// clock; nil means time.Now.
type Clock interface {
	Now() time.Time
}

// Request is one subscription. Scope, Projection and Instance identify the
// checkpoint/lease key. Definition is the projection's generated metadata
// (name and consumed event types) and must already be authorized by the
// caller: the core never resolves a catalogue, so the type set cannot be
// widened by a request. The caller also authorizes Scope/Projection/Instance;
// the core treats them as given.
type Request struct {
	Scope      string
	Projection string
	// Instance distinguishes concurrent subscriptions of one projection; the
	// empty instance is the default instance.
	Instance string
	Consumer string
	// Owner is the caller-supplied lease-owner token. Subscribe uses it
	// verbatim, so the consumer can present it in Ack.Owner and its acks are
	// fenced to its own lease. Empty means the server mints an opaque owner;
	// such a stream is not ackable — a minted owner is never disclosed to the
	// consumer, so no Ack.Owner can match its holder.
	Owner string
	// Definition supplies the authorized event type set Subscribe forwards to
	// Log.ReadAllBounded and the projection name used in logs/errors. Its
	// Hash() is the fingerprint the checkpoint must match: a stored
	// fingerprint from a different definition fails the subscription with
	// ErrDefinitionChanged instead of silently skipping history.
	Definition projection.Definition
	BatchSize  int
}

// Ack is one acknowledged delivery position: the consumer's receipt for the
// events it processed. It names the subscription the same way Request does,
// plus the Owner token of the subscription that processed them; Checkpoint
// fences the write on the lease holder, so only the live subscriber's
// receipts land.
type Ack struct {
	// Scope, Projection and Instance name the checkpoint to advance.
	Scope, Projection, Instance string
	// Owner is the acking subscription's Request.Owner token. It must equal
	// the lease holder's owner; a stream subscribed with an empty
	// Request.Owner has a server-minted, opaque owner and cannot ack.
	Owner string
	// Position is the highest global_position the consumer acknowledges.
	Position uint64
	// Definition is the acking consumer's definition hash
	// (Definition.Hash()). Checkpoint persists it as the checkpoint's
	// identity, so the first definition-carrying ack records what the
	// subscription must keep matching; an empty Definition preserves the
	// stored hash instead of clearing it.
	Definition string
}

// Sink receives events in global order. Send must block for backpressure and
// respect ctx; a non-nil error ends the subscription and is returned to the
// Subscribe caller. The feed does not save a checkpoint on Send: the consumer
// acknowledges through Server.Checkpoint, so a reconnect resumes from its last
// acknowledged position.
type Sink interface {
	Send(ctx context.Context, env es.Envelope) error
}

// Status is a subscription's observable state.
type Status struct {
	Position         uint64
	Head             uint64
	Lag              int64
	ActiveConsumer   string
	LastCheckpointAt time.Time
}
