package es

import (
	"time"

	"github.com/google/uuid"
)

// Envelope is the Go-side wrapper around every stored event. The Store
// persists and returns Envelopes; it treats Payload as opaque bytes
// tagged by TypeURL and never inspects it (docs/adr/0001, "storage is
// neutral").
//
// The field set is deliberately smaller than the parent framework's:
// no tenant, no crypto key-refs, no tamper-evidence hash chain. What
// remains is identity, ordering, schema, the two timestamps, and the
// audit/causality metadata that make the log a genuine audit trail.
type Envelope struct {
	// Identity & ordering
	EventID        uuid.UUID // UUIDv7 — globally unique, time-ordered
	StreamID       StreamID
	Version        uint64 // per-stream, 1-based, contiguous
	GlobalPosition uint64 // monotonic across all streams; the delivery cursor

	// Workspace is the source workspace, carried for delivery routing and
	// NATS subject construction (ADR 0003/0004). It is NOT part of stream
	// identity (StreamID stays Type:ID) — it is provenance the storage
	// adapter fills in. Empty for single-workspace backends (SQLite).
	Workspace string

	// Type & schema
	TypeURL       string // e.g. "counter.v1.Incremented"; the codec dispatch key
	SchemaVersion uint32 // event schema version (default 1); enables future upcasting

	// Time — two distinct clocks, both load-bearing for time-travel.
	OccurredAt time.Time // domain time, supplied by the command
	RecordedAt time.Time // DB commit time, set by the adapter; the as-of-time basis

	// Causality & audit
	CorrelationID uuid.UUID // groups all events from one originating request/flow
	CausationID   uuid.UUID // the event/command that directly caused this one
	CommandID     uuid.UUID // the command that produced this event
	Actor         Actor     // who caused it

	// Payload — canonical serialized event bytes (proto by convention).
	Payload []byte
}
