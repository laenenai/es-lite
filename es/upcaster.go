package es

// Upcaster transforms a decoded event from an older schema version to the
// current one, at read time. The aggregate runtime (and projection replays)
// run it after Codec.Decode when a stored event's SchemaVersion is older than
// the code's current version — see the schema_version option (ADR 0001).
//
// Like Decider.Evolve, an Upcaster runs during replay, so it must be pure and
// deterministic: no clock, no I/O, no randomness. Returning the event
// unchanged is valid (no upcast needed).
type Upcaster[E any] interface {
	// Upcast upgrades event to the current schema. typeURL and fromVersion
	// let one Upcaster route across event types and versions.
	Upcast(typeURL string, fromVersion uint32, event E) (E, error)
}
