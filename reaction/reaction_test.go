package reaction_test

import (
	"context"
	"errors"
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
