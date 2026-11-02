package es

import (
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	esv1 "github.com/laenenai/es-lite/gen/es/v1"
)

// MaxDecisionSkew is how far a command's client-supplied decided_at may
// differ from the server clock before aggregate.Runtime rejects it.
const MaxDecisionSkew = 5 * time.Minute

// DecidedAt is implemented by command variants whose .proto tags a
// google.protobuf.Timestamp field with (es.v1.decided_at) (ADR 0032).
// protoc-gen-es-lite emits the method. A zero value means the field was
// left unset.
//
// decided_at is the caller's intent, not authority. aggregate.Runtime
// validates it against the server clock and then stamps it with server
// time before Decide runs, so a decider that checks an expiry window reads
// the server's decision time (ADR 0034).
//
// The method is DecisionTime rather than DecidedAt because protoc-gen-go
// already emits a struct field named DecidedAt for a proto field called
// decided_at, and Go forbids a field and a method of the same name.
type DecidedAt interface {
	DecisionTime() time.Time
}

// DecisionTimeSetter is optionally implemented by a command whose decision
// time the runtime may replace with the server clock. Generated commands are
// stamped through proto reflection; this interface lets hand-written command
// types participate too.
type DecisionTimeSetter interface {
	SetDecisionTime(time.Time)
}

// StampDecisionTime replaces a command's tagged (es.v1.decided_at) field with
// at. It reports whether the command had a settable decision-time field. It is
// the mechanism aggregate.Runtime uses so Decide sees server time even when a
// decider reads the command's decided_at directly (ADR 0034).
func StampDecisionTime(cmd any, at time.Time) bool {
	if setter, ok := cmd.(DecisionTimeSetter); ok {
		setter.SetDecisionTime(at)
		return true
	}
	m, ok := cmd.(proto.Message)
	if !ok {
		return false
	}
	r := m.ProtoReflect()
	fields := r.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if fd.Kind() != protoreflect.MessageKind || fd.Message() == nil ||
			fd.Message().FullName() != "google.protobuf.Timestamp" {
			continue
		}
		opts := fd.Options()
		if opts == nil || !proto.HasExtension(opts, esv1.E_DecidedAt) {
			continue
		}
		on, _ := proto.GetExtension(opts, esv1.E_DecidedAt).(bool)
		if !on {
			continue
		}
		r.Set(fd, protoreflect.ValueOfMessage(timestamppb.New(at).ProtoReflect()))
		return true
	}
	return false
}
