// Package cmdsvc pins the runtime the generated Connect command-service
// handlers import (ADR 0031): the full es error -> Connect error table
// (MapError, with its VersionConflict detail), the header -> es.Meta
// mapping (MetaFrom, including the UUIDv5 hashing of non-UUID correlation
// and idempotency keys), the fail-closed DefaultValidator, and the
// VersionConflictDetail / SetWriteResult helpers.
package cmdsvc

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"buf.build/go/protovalidate"
	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	countersvcv1 "github.com/laenenai/es-lite/cmdsvc/example/gen/countersvc/v1"
	"github.com/laenenai/es-lite/es"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
	esv1 "github.com/laenenai/es-lite/gen/es/v1"
)

// headerOf builds a request header carrying exactly one value.
func headerOf(hdr, val string) http.Header {
	h := http.Header{}
	h.Set(hdr, val)
	return h
}

// TestMapErrorTable is the full sentinel -> connect code table MapError
// implements today, matched through wrapping (so it must go via errors.Is,
// never ==), plus the mapper and pass-through edges.
// es.ErrDuplicateEventID lands on aborted — IdempotencyConcurrent, HTTP
// 409 semantics — the concurrent duplicate of the Idempotency-Key flow
// (draft-ietf-httpapi-idempotency-key-header-07 §2.6, design D6).
func TestMapErrorTable(t *testing.T) {
	ctx := context.Background()

	t.Run("every mapped sentinel lands on its connect code", func(t *testing.T) {
		for _, tt := range []struct {
			name    string
			err     error
			code    connect.Code
			details int
		}{
			{es.ErrConflict.Error(), es.ErrConflict, connect.CodeFailedPrecondition, 1},
			{es.ErrTerminal.Error(), es.ErrTerminal, connect.CodeFailedPrecondition, 0},
			{es.ErrConstraintViolated.Error(), es.ErrConstraintViolated, connect.CodeAlreadyExists, 0},
			{es.ErrDuplicateEventID.Error(), es.ErrDuplicateEventID, connect.CodeAborted, 0},
			{es.ErrInvalidStreamID.Error(), es.ErrInvalidStreamID, connect.CodeInvalidArgument, 0},
			{es.ErrDecidedAtMissing.Error(), es.ErrDecidedAtMissing, connect.CodeInvalidArgument, 0},
			{es.ErrDecidedAtSkew.Error(), es.ErrDecidedAtSkew, connect.CodeInvalidArgument, 0},
			{"context.Canceled", context.Canceled, connect.CodeCanceled, 0},
			{"context.DeadlineExceeded", context.DeadlineExceeded, connect.CodeDeadlineExceeded, 0},
		} {
			wrapped := fmt.Errorf("handle: %w", tt.err)
			ce := MapError(ctx, wrapped, nil, nil, 0)
			if ce.Code() != tt.code {
				t.Errorf("%s: code = %s, want %s", tt.name, ce.Code(), tt.code)
			}
			if got := len(ce.Details()); got != tt.details {
				t.Errorf("%s: details = %d, want %d", tt.name, got, tt.details)
			}
		}
	})

	t.Run("conflict detail comes from the expected parameter", func(t *testing.T) {
		ce := MapError(ctx, es.ErrConflict, nil, nil, 7)
		if ce.Code() != connect.CodeFailedPrecondition {
			t.Fatalf("code = %s, want failed_precondition", ce.Code())
		}
		vc, ok := VersionConflictDetail(ce)
		if !ok {
			t.Fatal("no VersionConflict detail on the conflict error")
		}
		if vc.GetExpectedVersion() != 7 || vc.GetCurrentVersion() != 0 || vc.GetCurrentState() != nil {
			t.Fatalf("detail = expected %d current %d state %+v, want 7/0/nil",
				vc.GetExpectedVersion(), vc.GetCurrentVersion(), vc.GetCurrentState())
		}
	})

	t.Run("a *ConflictError overrides the expected version", func(t *testing.T) {
		sid, _ := es.NewStreamID("counter", "main")
		ce := MapError(ctx, fmt.Errorf("handle: %w", &es.ConflictError{StreamID: sid, Expected: 3, Current: 9}), nil, nil, 7)
		vc, ok := VersionConflictDetail(ce)
		if !ok || vc.GetExpectedVersion() != 3 || vc.GetCurrentVersion() != 9 {
			t.Fatalf("detail = %+v, want expected 3 current 9", vc)
		}
	})

	t.Run("a successful reload fills the current version and state", func(t *testing.T) {
		reload := func(context.Context) (uint64, proto.Message, error) {
			return 12, &counterv1.Counter{Count: 15}, nil
		}
		ce := MapError(ctx, es.ErrConflict, nil, reload, 5)
		vc, ok := VersionConflictDetail(ce)
		if !ok || vc.GetExpectedVersion() != 5 || vc.GetCurrentVersion() != 12 {
			t.Fatalf("detail = %+v, want expected 5 current 12", vc)
		}
		var state counterv1.Counter
		if err := vc.GetCurrentState().UnmarshalTo(&state); err != nil || state.GetCount() != 15 {
			t.Fatalf("current_state = %+v err = %v, want count 15", vc.GetCurrentState(), err)
		}
	})

	t.Run("a failing reload keeps the conflict's own current version", func(t *testing.T) {
		reload := func(context.Context) (uint64, proto.Message, error) { return 0, nil, errors.New("db down") }
		sid, _ := es.NewStreamID("counter", "main")
		ce := MapError(ctx, &es.ConflictError{StreamID: sid, Expected: 3, Current: 9}, nil, reload, 0)
		vc, ok := VersionConflictDetail(ce)
		if !ok || vc.GetExpectedVersion() != 3 || vc.GetCurrentVersion() != 9 || vc.GetCurrentState() != nil {
			t.Fatalf("detail = %+v, want expected 3 current 9 with no state", vc)
		}
	})

	// The precedence rule: the conflict's own current version is a floor
	// reload may advance but never retract. Reload is exported, so an
	// application can wire it to a replica or any other lagging read.
	t.Run("a reload behind the conflict's version is discarded whole", func(t *testing.T) {
		// A replica 5 versions behind. Answering "the stream is at v4" to a
		// client whose v3 assertion just lost at v9 is not an instruction
		// it can act on, and its state is as stale as its version.
		reload := func(context.Context) (uint64, proto.Message, error) {
			return 4, &counterv1.Counter{Count: 4}, nil
		}
		sid, _ := es.NewStreamID("counter", "main")
		ce := MapError(ctx, &es.ConflictError{StreamID: sid, Expected: 3, Current: 9}, nil, reload, 0)
		vc, ok := VersionConflictDetail(ce)
		if !ok || vc.GetExpectedVersion() != 3 || vc.GetCurrentVersion() != 9 {
			t.Fatalf("detail = %+v, want expected 3 current 9 — the exact version the append proved", vc)
		}
		if vc.GetCurrentState() != nil {
			t.Fatal("stale state attached as current_state; a lagging reload must be discarded whole, not split")
		}
	})

	t.Run("a reload level with the conflict's version wins and brings state", func(t *testing.T) {
		reload := func(context.Context) (uint64, proto.Message, error) {
			return 9, &counterv1.Counter{Count: 9}, nil
		}
		sid, _ := es.NewStreamID("counter", "main")
		ce := MapError(ctx, &es.ConflictError{StreamID: sid, Expected: 3, Current: 9}, nil, reload, 0)
		vc, ok := VersionConflictDetail(ce)
		if !ok || vc.GetCurrentVersion() != 9 {
			t.Fatalf("detail = %+v, want current 9", vc)
		}
		var state counterv1.Counter
		if err := vc.GetCurrentState().UnmarshalTo(&state); err != nil || state.GetCount() != 9 {
			t.Fatalf("current_state = %+v err = %v, want count 9 — a reload at the floor is coherent and must be kept",
				vc.GetCurrentState(), err)
		}
	})

	t.Run("a reload ahead of the conflict's version wins both fields", func(t *testing.T) {
		// The stream moved on between the rejected append and the reload.
		// That pair is fresher AND coherent, so it is the better answer.
		reload := func(context.Context) (uint64, proto.Message, error) {
			return 12, &counterv1.Counter{Count: 12}, nil
		}
		sid, _ := es.NewStreamID("counter", "main")
		ce := MapError(ctx, &es.ConflictError{StreamID: sid, Expected: 3, Current: 9}, nil, reload, 0)
		vc, ok := VersionConflictDetail(ce)
		if !ok || vc.GetCurrentVersion() != 12 {
			t.Fatalf("detail = %+v, want current 12", vc)
		}
		var state counterv1.Counter
		if err := vc.GetCurrentState().UnmarshalTo(&state); err != nil || state.GetCount() != 12 {
			t.Fatalf("current_state = %+v err = %v, want count 12", vc.GetCurrentState(), err)
		}
	})

	t.Run("the app's mapper maps what it claims", func(t *testing.T) {
		mapper := func(error) *connect.Error {
			return connect.NewError(connect.CodeOutOfRange, errors.New("out of range"))
		}
		ce := MapError(ctx, errors.New("domain rejection"), mapper, nil, 0)
		if ce.Code() != connect.CodeOutOfRange {
			t.Fatalf("code = %s, want out_of_range", ce.Code())
		}
	})

	t.Run("a mapper returning nil falls back to failed_precondition", func(t *testing.T) {
		mapper := func(error) *connect.Error { return nil }
		ce := MapError(ctx, errors.New("domain rejection"), mapper, nil, 0)
		if ce.Code() != connect.CodeFailedPrecondition {
			t.Fatalf("code = %s, want failed_precondition", ce.Code())
		}
	})

	t.Run("a nil mapper falls back to failed_precondition", func(t *testing.T) {
		ce := MapError(ctx, errors.New("domain rejection"), nil, nil, 0)
		if ce.Code() != connect.CodeFailedPrecondition {
			t.Fatalf("code = %s, want failed_precondition", ce.Code())
		}
	})

	t.Run("a connect error passes through untouched", func(t *testing.T) {
		sentinel := connect.NewError(connect.CodeNotFound, errors.New("not here"))
		mapper := func(error) *connect.Error {
			return connect.NewError(connect.CodeInternal, errors.New("mapper must not run"))
		}
		ce := MapError(ctx, fmt.Errorf("handle: %w", sentinel), mapper, nil, 0)
		if ce != sentinel {
			t.Fatalf("pass-through returned %v, want the original error unmodified", ce)
		}
	})
}

// testNamespace is idempotencyNamespace's literal, pinned here so an
// accidental namespace change — which rotates every derived id in the audit
// trail — fails this suite instead of silently reshaping recorded ids.
var testNamespace = uuid.MustParse("3b5a0c2e-7f5d-4b1a-9c3e-2d6f8a1b4c7e")

func TestMetaFrom(t *testing.T) {
	actor := es.Actor{Type: "user", ID: "alice"}

	t.Run("a UUID correlation id passes through byte-for-byte", func(t *testing.T) {
		m := MetaFrom(headerOf(HdrCorrelationID, "0192f3a0-0000-7000-8000-0000000000c0"), actor)
		if want := uuid.MustParse("0192f3a0-0000-7000-8000-0000000000c0"); m.CorrelationID != want {
			t.Fatalf("correlation id = %s, want %s", m.CorrelationID, want)
		}
		if m.Actor != actor {
			t.Fatalf("actor = %+v, want %+v", m.Actor, actor)
		}
		if m.CommandID != (uuid.UUID{}) {
			t.Fatalf("command id = %s, want zero (no Idempotency-Key header)", m.CommandID)
		}
	})

	t.Run("a non-UUID correlation id becomes a stable UUIDv5", func(t *testing.T) {
		h := headerOf(HdrCorrelationID, "order-42")
		m1, m2 := MetaFrom(h, actor), MetaFrom(h, actor)
		want := uuid.NewSHA1(testNamespace, []byte("order-42"))
		if m1.CorrelationID == (uuid.UUID{}) {
			t.Fatal("non-UUID correlation id dropped: Meta.CorrelationID is zero")
		}
		if m1.CorrelationID != want {
			t.Fatalf("correlation id = %s, want the UUIDv5 %s over the idempotency namespace", m1.CorrelationID, want)
		}
		if m1.CorrelationID != m2.CorrelationID {
			t.Fatalf("correlation id not stable across calls: %s vs %s", m1.CorrelationID, m2.CorrelationID)
		}
		if v := m1.CorrelationID.Version(); v != 5 {
			t.Fatalf("correlation id version = %d, want 5", v)
		}
		if other := MetaFrom(headerOf(HdrCorrelationID, "order-43"), actor); other.CorrelationID == m1.CorrelationID {
			t.Fatal("distinct correlation strings must not hash to the same id")
		}
	})

	t.Run("a UUID idempotency key passes through", func(t *testing.T) {
		m := MetaFrom(headerOf(HdrIdempotencyKey, "0192f3a0-0000-7000-8000-0000000000c1"), actor)
		if want := uuid.MustParse("0192f3a0-0000-7000-8000-0000000000c1"); m.CommandID != want {
			t.Fatalf("command id = %s, want %s", m.CommandID, want)
		}
		if m.CorrelationID != (uuid.UUID{}) {
			t.Fatalf("correlation id = %s, want zero (no Correlation-Id header)", m.CorrelationID)
		}
	})

	t.Run("a string idempotency key becomes a stable UUIDv5", func(t *testing.T) {
		h := headerOf(HdrIdempotencyKey, "order-42")
		m1, m2 := MetaFrom(h, actor), MetaFrom(h, actor)
		want := uuid.NewSHA1(testNamespace, []byte("order-42"))
		if m1.CommandID != want {
			t.Fatalf("command id = %s, want the UUIDv5 %s over the idempotency namespace", m1.CommandID, want)
		}
		if m1.CommandID != m2.CommandID {
			t.Fatalf("command id not stable across calls: %s vs %s (retries would not share a CommandID)", m1.CommandID, m2.CommandID)
		}
		if other := MetaFrom(headerOf(HdrIdempotencyKey, "order-43"), actor); other.CommandID == m1.CommandID {
			t.Fatal("distinct idempotency keys must not hash to the same command id")
		}
	})

	t.Run("empty headers leave the ids zero for the runtime to fill", func(t *testing.T) {
		m := MetaFrom(http.Header{}, actor)
		if m.CorrelationID != (uuid.UUID{}) || m.CommandID != (uuid.UUID{}) {
			t.Fatalf("meta = %+v, want zero correlation and command ids", m)
		}
		if m.Actor != actor {
			t.Fatalf("actor = %+v, want %+v", m.Actor, actor)
		}
	})
}

func TestDefaultValidator(t *testing.T) {
	validMsgs := []proto.Message{
		&countersvcv1.WriteResult{StreamId: "counter:main", Version: 1, GlobalPosition: 1},
		&countersvcv1.IncrementRequest{CounterId: "main", Command: &counterv1.Increment{By: 1}},
	}
	invalidMsg := &countersvcv1.IncrementRequest{CounterId: strings.Repeat("a", 17), Command: &counterv1.Increment{By: 1}}

	t.Run("the protovalidate path accepts valid and rejects invalid", func(t *testing.T) {
		v := DefaultValidator()
		for i, msg := range validMsgs {
			if err := v.Validate(msg); err != nil {
				t.Fatalf("valid message %d: %v", i, err)
			}
		}
		err := v.Validate(invalidMsg)
		var ve *protovalidate.ValidationError
		if !errors.As(err, &ve) {
			t.Fatalf("invalid message: %v, want a protovalidate ValidationError", err)
		}
	})

	t.Run("a constructor failure fails closed on EVERY Validate call", func(t *testing.T) {
		sentinel := errors.New("cel environment exploded")
		v := newValidator(func() (protovalidate.Validator, error) { return nil, sentinel })
		// The first two messages are ones the protovalidate path accepts —
		// a fail-open validator would let them through.
		for i, msg := range append(append([]proto.Message{}, validMsgs...), invalidMsg) {
			err := v.Validate(msg)
			if err == nil {
				t.Fatalf("message %d: Validate = nil, want the constructor error (fail closed)", i)
			}
			if !errors.Is(err, sentinel) {
				t.Fatalf("message %d: %v, want the constructor error", i, err)
			}
			if !strings.Contains(err.Error(), "cmdsvc: protovalidate:") {
				t.Fatalf("message %d: %q, want the cmdsvc: protovalidate: prefix", i, err)
			}
		}
	})
}

func TestVersionConflictDetail(t *testing.T) {
	state, err := anypb.New(&counterv1.Counter{Count: 15})
	if err != nil {
		t.Fatal(err)
	}
	ce := connect.NewError(connect.CodeFailedPrecondition, errors.New("stale"))
	detail, err := connect.NewErrorDetail(&esv1.VersionConflict{ExpectedVersion: 3, CurrentVersion: 9, CurrentState: state})
	if err != nil {
		t.Fatal(err)
	}
	ce.AddDetail(detail)

	t.Run("the detail round-trips", func(t *testing.T) {
		got, ok := VersionConflictDetail(ce)
		if !ok {
			t.Fatal("VersionConflict detail not found")
		}
		if got.GetExpectedVersion() != 3 || got.GetCurrentVersion() != 9 {
			t.Fatalf("detail = %+v, want expected 3 current 9", got)
		}
		var round counterv1.Counter
		if err := got.GetCurrentState().UnmarshalTo(&round); err != nil || round.GetCount() != 15 {
			t.Fatalf("current_state = %+v err = %v, want count 15", got.GetCurrentState(), err)
		}
	})

	t.Run("non-matching shapes report absence", func(t *testing.T) {
		if _, ok := VersionConflictDetail(errors.New("plain")); ok {
			t.Fatal("a non-connect error carries no detail")
		}
		if _, ok := VersionConflictDetail(connect.NewError(connect.CodeInternal, errors.New("bare"))); ok {
			t.Fatal("a detail-less connect error carries no VersionConflict")
		}
		other := connect.NewError(connect.CodeInternal, errors.New("other detail"))
		if od, oerr := connect.NewErrorDetail(timestamppb.Now()); oerr == nil {
			other.AddDetail(od)
		}
		if _, ok := VersionConflictDetail(other); ok {
			t.Fatal("a different detail type must not read as a VersionConflict")
		}
	})
}

func TestSetWriteResult(t *testing.T) {
	sid := es.StreamID{Type: "counter", ID: "main"}

	t.Run("fills the three WriteResult fields", func(t *testing.T) {
		wr := &countersvcv1.WriteResult{}
		if err := SetWriteResult(wr, sid, 3, 9); err != nil {
			t.Fatal(err)
		}
		if wr.GetStreamId() != "counter:main" || wr.GetVersion() != 3 || wr.GetGlobalPosition() != 9 {
			t.Fatalf("write result = %+v, want counter:main v3 @9", wr)
		}
	})

	t.Run("a message without the fields errors naming the first missing one", func(t *testing.T) {
		err := SetWriteResult(&countersvcv1.IncrementRequest{}, sid, 1, 1)
		if err == nil || !strings.Contains(err.Error(), `"stream_id"`) {
			t.Fatalf("err = %v, want a missing stream_id error", err)
		}
	})

	t.Run("a message with a stream_id but no version errors", func(t *testing.T) {
		msg := dynamicShape(t, "StreamOnly", []*descriptorpb.FieldDescriptorProto{
			stringField("stream_id", 1),
		})
		err := SetWriteResult(msg, sid, 1, 1)
		if err == nil || !strings.Contains(err.Error(), `"version"`) {
			t.Fatalf("err = %v, want a missing version error", err)
		}
	})

	t.Run("a mistyped version field errors", func(t *testing.T) {
		msg := dynamicShape(t, "StringVersion", []*descriptorpb.FieldDescriptorProto{
			stringField("stream_id", 1),
			stringField("version", 2), // a string where a uint64 belongs
		})
		err := SetWriteResult(msg, sid, 1, 1)
		if err == nil || !strings.Contains(err.Error(), `"version"`) {
			t.Fatalf("err = %v, want a mistyped version error", err)
		}
	})
}

func stringField(name string, num int32) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(num),
		Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
	}
}

// dynamicShape builds an in-memory proto message with exactly the fields
// listed, for response shapes no generated message in the repo carries.
func dynamicShape(t *testing.T, name string, fields []*descriptorpb.FieldDescriptorProto) proto.Message {
	t.Helper()
	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("cmdsvc_test_" + strings.ToLower(name) + ".proto"),
		Package: proto.String("cmdsvc.test"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name:  proto.String(name),
			Field: fields,
		}},
	}
	file, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		t.Fatal(err)
	}
	desc := file.Messages().ByName(protoreflect.Name(name))
	if desc == nil {
		t.Fatalf("descriptor %s not built", name)
	}
	return dynamicpb.NewMessage(desc)
}
