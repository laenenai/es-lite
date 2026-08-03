package es

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Store is the storage contract every backend implements. It is small
// and serialization-neutral: it moves Envelopes (opaque Payload +
// metadata) in and out, and knows nothing about aggregates, proto, or
// projections. Adapters live under sqlite/ (and, later, postgres/).
//
// The event log is append-only and immutable: there is no Update or
// Delete. History, auditability, and time-travel all rest on that
// invariant (docs/adr/0001).
type Store interface {
	// Append commits one batch of events for a single stream in one
	// transaction. It enforces optimistic concurrency against
	// p.ExpectedVersion and returns ErrConflict on mismatch. The
	// returned Envelopes are fully populated (Version, GlobalPosition,
	// RecordedAt, EventID).
	Append(ctx context.Context, p AppendParams) (AppendResult, error)

	// ReadStream returns a stream's events with version in the half-open
	// range (fromVersion, toVersion], ordered ascending. fromVersion=0
	// starts at the beginning; toVersion=0 means "no upper bound". So
	// ReadStream(sid, 0, 0) is the whole stream, and ReadStream(sid, 0, N)
	// is the prefix used for time-travel-by-version.
	ReadStream(ctx context.Context, sid StreamID, fromVersion, toVersion uint64) ([]Envelope, error)

	// ReadStreamAsOf returns a stream's events with RecordedAt <= asOf,
	// ordered ascending — the basis for time-travel-by-wall-clock.
	// RecordedAt (DB commit time) is used, not OccurredAt, because it is
	// the reproducible, monotonic clock.
	ReadStreamAsOf(ctx context.Context, sid StreamID, asOf time.Time) ([]Envelope, error)

	// ReadAll returns up to limit events with GlobalPosition >
	// fromPosition across all streams, ordered ascending. This is the
	// delivery cursor the poller tails — the log acting as the outbox.
	ReadAll(ctx context.Context, fromPosition uint64, limit int) ([]Envelope, error)

	// CurrentStreamVersion returns the highest committed version for a
	// stream, or 0 if the stream has no events.
	CurrentStreamVersion(ctx context.Context, sid StreamID) (uint64, error)

	// LookupClaim returns the stream that currently holds the uniqueness
	// claim (scope, value) — the reverse of Claim. `value` and `pii` are the
	// same as passed to Claim; the store keys the value identically, so a PII
	// claim is looked up by the same keyed HMAC. `found` is false when no
	// stream holds it (never claimed, or since released).
	//
	// This exposes the uniqueness index as a read: any aggregate that Claims a
	// value gets reverse lookup (e.g. external-identity → user_id) with
	// read-your-writes consistency, since the claim commits in the append
	// transaction — no separate, eventually-consistent projection needed.
	LookupClaim(ctx context.Context, scope, value string, pii bool) (streamID string, found bool, err error)
}

// AppendParams is the input to Store.Append. The audit/causality fields
// are command-scoped: they apply to every event in the batch, since a
// batch is the events produced by one command.
type AppendParams struct {
	StreamID StreamID

	// ExpectedVersion is the stream version the caller believes is
	// current. 0 means "the stream must not yet exist". A mismatch with
	// the stored version yields ErrConflict.
	ExpectedVersion uint64

	// Events to append, in order. Each becomes version
	// ExpectedVersion+1, +2, ...
	Events []EventData

	// Constraints are uniqueness Claim/Release operations applied in the
	// same transaction as the events (ADR 0008). A colliding Claim rolls the
	// whole append back with ErrConstraintViolated.
	Constraints []ConstraintOp

	// Command-scoped audit metadata, copied onto every produced Envelope.
	CommandID     uuid.UUID
	CorrelationID uuid.UUID
	CausationID   uuid.UUID
	Actor         Actor
	OccurredAt    time.Time // domain time; adapter uses now if zero
}

// EventData is one event to append: its encoded form plus an optional
// caller-supplied EventID (the adapter generates a UUIDv7 when zero).
type EventData struct {
	EventID       uuid.UUID
	TypeURL       string
	SchemaVersion uint32
	Payload       []byte
}

// AppendResult reports what was committed.
type AppendResult struct {
	FromVersion uint64 // stream version before the append (== ExpectedVersion)
	ToVersion   uint64 // stream version after the append
	Envelopes   []Envelope
}
