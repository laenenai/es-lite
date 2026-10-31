// Package cmdsvc is the runtime the generated Connect command-service
// handlers import (ADR 0031): request validation, identity and audit
// metadata, the Result -> WriteResult mapping and the es error -> Connect
// error mapping with a VersionConflict detail. It is a nested Go module so
// connect-go and protovalidate stay out of the core module's graph.
package cmdsvc

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"buf.build/go/protovalidate"
	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/laenenai/es-lite/es"
	esv1 "github.com/laenenai/es-lite/gen/es/v1"
)

// Request headers the generated handler reads into es.Meta.
const (
	HdrCorrelationID  = "Correlation-Id"
	HdrIdempotencyKey = "Idempotency-Key"
)

// Validator checks a request message. DefaultValidator is protovalidate.
type Validator interface {
	Validate(msg proto.Message) error
}

// ValidatorFunc adapts a function to Validator.
type ValidatorFunc func(msg proto.Message) error

// Validate implements Validator.
func (f ValidatorFunc) Validate(msg proto.Message) error { return f(msg) }

// DefaultValidator returns a protovalidate-backed Validator. Construction
// cannot fail for the standard constraints; an unexpected failure yields a
// Validator that rejects every request with that error rather than one that
// silently allows.
func DefaultValidator() Validator {
	v, err := protovalidate.New()
	if err != nil {
		return ValidatorFunc(func(proto.Message) error { return fmt.Errorf("cmdsvc: protovalidate: %w", err) })
	}
	return ValidatorFunc(func(msg proto.Message) error { return v.Validate(msg) })
}

// ErrorMapper lets the application map its own domain errors (decider
// rejections) to Connect errors. Return nil to fall through to the default
// (failed_precondition).
type ErrorMapper func(err error) *connect.Error

// Reload fetches a stream's current version and filtered state for a
// VersionConflict detail; the generated handler supplies it.
type Reload func(ctx context.Context) (version uint64, state proto.Message, err error)

// MetaFrom builds the command's audit metadata from the request headers and
// the authenticated actor: Correlation-Id (a UUID) and Idempotency-Key (a
// UUID, or any string hashed to a UUIDv5 so retries share a CommandID).
// OccurredAt is left zero so ADR 0032's decided_at rule fills it.
func MetaFrom(h http.Header, actor es.Actor) es.Meta {
	m := es.Meta{Actor: actor}
	if v := h.Get(HdrCorrelationID); v != "" {
		if id, err := uuid.Parse(v); err == nil {
			m.CorrelationID = id
		}
	}
	if v := h.Get(HdrIdempotencyKey); v != "" {
		if id, err := uuid.Parse(v); err == nil {
			m.CommandID = id
		} else {
			m.CommandID = uuid.NewSHA1(idempotencyNamespace, []byte(v))
		}
	}
	return m
}

// idempotencyNamespace is the fixed UUIDv5 namespace for non-UUID keys.
var idempotencyNamespace = uuid.MustParse("3b5a0c2e-7f5d-4b1a-9c3e-2d6f8a1b4c7e")

// InvalidArgument maps a validation or parse failure. A protovalidate
// ValidationError attaches its violations as an error detail.
func InvalidArgument(err error) *connect.Error {
	ce := connect.NewError(connect.CodeInvalidArgument, err)
	var ve *protovalidate.ValidationError
	if errors.As(err, &ve) {
		if d, derr := connect.NewErrorDetail(ve.ToProto()); derr == nil {
			ce.AddDetail(d)
		}
	}
	return ce
}

// MissingCommand reports a request whose command field is unset.
func MissingCommand(field string) *connect.Error {
	return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("cmdsvc: %s is required", field))
}

// Unauthenticated maps an identity-source failure; a *connect.Error passes through.
func Unauthenticated(err error) *connect.Error { return passOr(connect.CodeUnauthenticated, err) }

// PermissionDenied maps an authorizer refusal; a *connect.Error passes through.
func PermissionDenied(err error) *connect.Error { return passOr(connect.CodePermissionDenied, err) }

// Internal maps an unexpected handler-side failure.
func Internal(err error) *connect.Error { return connect.NewError(connect.CodeInternal, err) }

func passOr(code connect.Code, err error) *connect.Error {
	var ce *connect.Error
	if errors.As(err, &ce) {
		return ce
	}
	return connect.NewError(code, err)
}

// MapError maps a Runtime.Handle error to a Connect error (ADR 0031 §2):
//
//	es.ErrConflict            failed_precondition + VersionConflict detail (via reload)
//	es.ErrTerminal            failed_precondition
//	es.ErrConstraintViolated  already_exists
//	es.ErrInvalidStreamID,
//	es.ErrDecidedAtMissing,
//	es.ErrDecidedAtMismatch   invalid_argument
//	anything else             mapper(err) if it returns non-nil, else failed_precondition
//
// expected is the version the command asserted (0 if none); a
// *es.ConflictError in err overrides it.
func MapError(ctx context.Context, err error, mapper ErrorMapper, reload Reload, expected uint64) *connect.Error {
	var ce *connect.Error
	if errors.As(err, &ce) {
		return ce
	}
	switch {
	case errors.Is(err, es.ErrConflict):
		out := connect.NewError(connect.CodeFailedPrecondition, err)
		detail := &esv1.VersionConflict{ExpectedVersion: expected}
		var conflict *es.ConflictError
		if errors.As(err, &conflict) {
			detail.ExpectedVersion, detail.CurrentVersion = conflict.Expected, conflict.Current
		}
		if reload != nil {
			if version, state, rerr := reload(ctx); rerr == nil {
				detail.CurrentVersion = version
				if state != nil {
					if a, aerr := anypb.New(state); aerr == nil {
						detail.CurrentState = a
					}
				}
			}
		}
		if d, derr := connect.NewErrorDetail(detail); derr == nil {
			out.AddDetail(d)
		}
		return out
	case errors.Is(err, es.ErrTerminal):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, es.ErrConstraintViolated):
		return connect.NewError(connect.CodeAlreadyExists, err)
	case errors.Is(err, es.ErrInvalidStreamID), errors.Is(err, es.ErrDecidedAtMissing), errors.Is(err, es.ErrDecidedAtMismatch):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, context.Canceled):
		return connect.NewError(connect.CodeCanceled, err)
	case errors.Is(err, context.DeadlineExceeded):
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	}
	if mapper != nil {
		if ce := mapper(err); ce != nil {
			return ce
		}
	}
	return connect.NewError(connect.CodeFailedPrecondition, err)
}

// VersionConflictDetail extracts the VersionConflict detail from a Connect
// error, if present — the client-side counterpart of MapError.
func VersionConflictDetail(err error) (*esv1.VersionConflict, bool) {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return nil, false
	}
	for _, d := range ce.Details() {
		msg, verr := d.Value()
		if verr != nil {
			continue
		}
		if vc, ok := msg.(*esv1.VersionConflict); ok {
			return vc, true
		}
	}
	return nil, false
}

// SetWriteResult fills the three WriteResult fields by name on any message
// that has them (contracts' ont.api.common.v1.WriteResult, or a service's
// own): stream_id (string), version (uint64), global_position (uint64). The
// message type is opaque to es-lite; a missing or mistyped field is an error.
func SetWriteResult(result proto.Message, sid es.StreamID, version, globalPosition uint64) error {
	m := result.ProtoReflect()
	fields := m.Descriptor().Fields()
	set := func(name protoreflect.Name, kind protoreflect.Kind, v protoreflect.Value) error {
		fd := fields.ByName(name)
		if fd == nil || fd.Kind() != kind || fd.IsList() {
			return fmt.Errorf("cmdsvc: %s has no singular %s field %q", m.Descriptor().FullName(), kind, name)
		}
		m.Set(fd, v)
		return nil
	}
	if err := set("stream_id", protoreflect.StringKind, protoreflect.ValueOfString(sid.Canonical())); err != nil {
		return err
	}
	if err := set("version", protoreflect.Uint64Kind, protoreflect.ValueOfUint64(version)); err != nil {
		return err
	}
	return set("global_position", protoreflect.Uint64Kind, protoreflect.ValueOfUint64(globalPosition))
}

// LastGlobalPosition is the global_position of the last envelope, or 0 for
// a zero-event no-op (legal per contracts §3.7).
func LastGlobalPosition(envs []es.Envelope) uint64 {
	if len(envs) == 0 {
		return 0
	}
	return envs[len(envs)-1].GlobalPosition
}
