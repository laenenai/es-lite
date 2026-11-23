package es_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/es-lite/es"
)

func validMultiAppend(t *testing.T, name string, command uuid.UUID) es.AppendParams {
	t.Helper()
	sid, err := es.NewStreamID("counter", name)
	if err != nil {
		t.Fatalf("stream id %q: %v", name, err)
	}
	return es.AppendParams{
		StreamID: sid,
		Events: []es.EventData{{
			TypeURL: "counter.v1.Incremented",
			Payload: []byte("x"),
		}},
		CommandID: command,
		Actor:     es.Actor{Type: "user", ID: "u1"},
	}
}

func TestValidateMultiAppend(t *testing.T) {
	command := uuid.New()
	a := validMultiAppend(t, "a", command)
	b := validMultiAppend(t, "b", command)

	if err := es.ValidateMultiAppend(es.MultiAppend{}); !errors.Is(err, es.ErrEmptyMultiAppend) {
		t.Errorf("empty appends: err = %v, want ErrEmptyMultiAppend", err)
	}

	noEvents := a
	noEvents.Events = nil
	if err := es.ValidateMultiAppend(es.MultiAppend{Appends: []es.AppendParams{noEvents}}); !errors.Is(err, es.ErrEmptyMultiAppend) {
		t.Errorf("zero-event append: err = %v, want ErrEmptyMultiAppend", err)
	}

	duplicate := b
	duplicate.StreamID = a.StreamID
	if err := es.ValidateMultiAppend(es.MultiAppend{Appends: []es.AppendParams{a, duplicate}}); !errors.Is(err, es.ErrDuplicateMultiStream) {
		t.Errorf("duplicate stream: err = %v, want ErrDuplicateMultiStream", err)
	}

	differentCommand := b
	differentCommand.CommandID = uuid.New()
	if err := es.ValidateMultiAppend(es.MultiAppend{Appends: []es.AppendParams{a, differentCommand}}); !errors.Is(err, es.ErrConflictingMultiAppend) {
		t.Errorf("different command id: err = %v, want ErrConflictingMultiAppend", err)
	}

	differentActor := b
	differentActor.Actor = es.Actor{Type: "user", ID: "u2"}
	if err := es.ValidateMultiAppend(es.MultiAppend{Appends: []es.AppendParams{a, differentActor}}); !errors.Is(err, es.ErrConflictingMultiAppend) {
		t.Errorf("different actor: err = %v, want ErrConflictingMultiAppend", err)
	}

	differentSpan := b
	differentSpan.SpanContext = es.SpanContext{TraceID: [16]byte{1}, SpanID: [8]byte{2}, Flags: 1}
	if err := es.ValidateMultiAppend(es.MultiAppend{Appends: []es.AppendParams{a, differentSpan}}); !errors.Is(err, es.ErrConflictingMultiAppend) {
		t.Errorf("different span: err = %v, want ErrConflictingMultiAppend", err)
	}

	at := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
	c := validMultiAppend(t, "c", command)
	atB := b
	atB.OccurredAt = at
	atC := c
	atC.OccurredAt = at.Add(time.Second)
	if err := es.ValidateMultiAppend(es.MultiAppend{Appends: []es.AppendParams{atB, atC}}); !errors.Is(err, es.ErrConflictingMultiAppend) {
		t.Errorf("different occurred_at: err = %v, want ErrConflictingMultiAppend", err)
	}

	// A zero first append does not excuse later appends from agreeing with
	// each other: two non-zero times after a zero one are still a conflict.
	if err := es.ValidateMultiAppend(es.MultiAppend{Appends: []es.AppendParams{a, atB, atC}}); !errors.Is(err, es.ErrConflictingMultiAppend) {
		t.Errorf("zero first, disagreeing occurred_at: err = %v, want ErrConflictingMultiAppend", err)
	}
	zeroC := c
	zeroC.OccurredAt = time.Time{}
	if err := es.ValidateMultiAppend(es.MultiAppend{Appends: []es.AppendParams{a, atB, zeroC}}); err != nil {
		t.Errorf("zero first, one non-zero occurred_at: err = %v, want nil", err)
	}

	atC2 := c
	atC2.OccurredAt = at
	if err := es.ValidateMultiAppend(es.MultiAppend{Appends: []es.AppendParams{atB, atC2}}); err != nil {
		t.Errorf("same occurred_at: err = %v, want nil", err)
	}

	if err := es.ValidateMultiAppend(es.MultiAppend{Appends: []es.AppendParams{a, b}}); err != nil {
		t.Errorf("valid multi-append: err = %v, want nil", err)
	}
}
