package reaction_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/reaction"
)

type call struct {
	sid es.StreamID
	cmd string
}

func TestExecuteTerminalIsDoneOtherErrorsStop(t *testing.T) {
	var calls []call
	boom := errors.New("boom")
	d := reaction.DispatcherFunc[string](func(_ context.Context, sid es.StreamID, cmd string, meta es.Meta) error {
		calls = append(calls, call{sid, cmd})
		switch cmd {
		case "terminal":
			return es.ErrTerminal
		case "fail":
			return boom
		}
		return nil
	})
	a, _ := es.NewStreamID("t", "a")
	issues := []reaction.Issue[string]{{a, "ok"}, {a, "terminal"}, {a, "fail"}, {a, "never"}}
	err := reaction.Execute(context.Background(), d, issues, es.Meta{})
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want boom", err)
	}
	if len(calls) != 3 {
		t.Fatalf("calls = %v, want ok, terminal, fail", calls)
	}
}

func TestMetaFor(t *testing.T) {
	env := es.Envelope{EventID: uuid.New(), CorrelationID: uuid.New()}
	m := reaction.MetaFor(env, es.Actor{Type: "system", ID: "x"})
	if m.CausationID != env.EventID || m.CorrelationID != env.CorrelationID || m.Actor.ID != "x" || m.CommandID != uuid.Nil {
		t.Fatalf("meta = %+v", m)
	}
}

// TestExecuteRedeliveryRecollectsIssues pins the redelivery contract
// (ADR 0030 §3, rule 20): a reaction derives its issues from the
// envelope on every delivery, and nothing about a failed pass consumes
// or commits the collection. When dispatch fails after one issue
// already landed, the error propagates — so the runner naks and the
// event is redelivered — and a second Execute for the same envelope
// re-collects the same issues and re-dispatches from the top. The
// landed command is re-presented to the dispatcher (rule 20 makes the
// decider no-op for it), never silently skipped: issue collection is
// not committed-then-lost on handler failure.
func TestExecuteRedeliveryRecollectsIssues(t *testing.T) {
	a, _ := es.NewStreamID("t", "a")
	b, _ := es.NewStreamID("t", "b")
	env := es.Envelope{EventID: uuid.New(), CorrelationID: uuid.New()}
	meta := reaction.MetaFor(env, es.Actor{Type: "system", ID: "runner"})

	// collect models the generated handler's issue derivation: a pure
	// function of the envelope, so every delivery derives the same two
	// issues.
	collect := func() []reaction.Issue[string] {
		return []reaction.Issue[string]{
			{StreamID: a, Command: "first"},
			{StreamID: b, Command: "second"},
		}
	}

	boom := errors.New("boom")
	var delivered []call
	d := reaction.DispatcherFunc[string](func(_ context.Context, sid es.StreamID, cmd string, _ es.Meta) error {
		delivered = append(delivered, call{sid, cmd})
		if cmd == "second" && len(delivered) == 2 {
			return boom // fails on the first pass only
		}
		return nil
	})

	// First delivery: "first" lands, "second" fails, the error propagates.
	if err := reaction.Execute(context.Background(), d, collect(), meta); !errors.Is(err, boom) {
		t.Fatalf("first Execute: error = %v, want boom", err)
	}

	// Second delivery of the same envelope: the issues are re-collected
	// and Execute re-dispatches from the top — "first" included.
	if err := reaction.Execute(context.Background(), d, collect(), meta); err != nil {
		t.Fatalf("second Execute: error = %v, want nil", err)
	}

	want := []call{{a, "first"}, {b, "second"}, {a, "first"}, {b, "second"}}
	if !slices.Equal(delivered, want) {
		t.Fatalf("dispatches = %v, want %v", delivered, want)
	}
}
