package es

import "time"

// DecidedAt is implemented by command variants whose .proto tags a
// google.protobuf.Timestamp field with (es.v1.decided_at) (ADR 0032).
// protoc-gen-es-lite emits the method; aggregate.Runtime.Handle reads it to
// source Envelope.OccurredAt from the command, so the envelope's domain time
// and the time the decider reasoned about can never disagree. A zero value
// means the field was left unset.
//
// The method is DecisionTime rather than DecidedAt because protoc-gen-go
// already emits a struct field named DecidedAt for a proto field called
// decided_at, and Go forbids a field and a method of the same name.
type DecidedAt interface {
	DecisionTime() time.Time
}
