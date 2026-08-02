// Package upcast is a worked example of schema evolution in es-lite:
// bumping an event's (es.v1.schema_version) and registering an upcaster
// that upgrades older stored events to the current shape on read.
//
// The scenario (see upcast_test.go): the Counter's Incremented event
// originally carried `by` in whole units (schema version 1). A later
// change reinterprets the semantics — say the field is now understood in
// half-units — so the event is bumped to schema version 2 in the .proto:
//
//	message Incremented {
//	  option (es.v1.schema_version) = 2;
//	  int64 by = 1;
//	}
//
// Events already written at version 1 must be reinterpreted when replayed.
// An upcast.Registry maps the event's type URL to a function that, given
// the stored fromVersion, upgrades the decoded event. The aggregate
// runtime applies it after Codec.Decode, before Decider.Evolve — so the
// business logic only ever sees current-schema events, and the raw log
// stays immutable (nothing is rewritten).
//
// The example runs as a test (go test ./examples/upcast/...).
package upcast
