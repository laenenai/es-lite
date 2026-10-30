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
// no tenant and no crypto key-refs. What remains is identity, ordering,
// schema, the two timestamps, the audit/causality metadata that make the
// log a genuine audit trail, and — since ADR 0029 — the per-stream hash
// chain that makes it tamper-evident.
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

	// Tamper evidence (ADR 0029): a per-stream SHA-256 chain the store
	// computes inside the append transaction. PrevHash is the Hash of
	// version-1 (ZeroHash for version 1; UnchainedHeadHash for the first
	// chained event of a stream that predates the chain) and Hash is
	// EnvelopeHash of this envelope. Both are nil on rows written before
	// migration 0002. Store-assigned, like GlobalPosition and RecordedAt;
	// never supplied by the caller.
	PrevHash []byte
	Hash     []byte
}
