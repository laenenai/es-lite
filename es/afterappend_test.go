package es_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/laenenai/es-lite/es"
)

// appendStub is a minimal es.Store whose Append returns a configurable
// result and records each call, so the WithAfterAppend tests can pin the
// decorator's contract without a real backend.
type appendStub struct {
	es.Store
	appends  int
	reads    int
	result   es.AppendResult
	err      error
	gotParam es.AppendParams
}

func (s *appendStub) Append(_ context.Context, p es.AppendParams) (es.AppendResult, error) {
	s.appends++
	s.gotParam = p
	return s.result, s.err
}

func (s *appendStub) ReadStream(_ context.Context, _ es.StreamID, _, _ uint64) ([]es.Envelope, error) {
	s.reads++
	return nil, nil
}

func TestWithAfterAppendFiresOnceWithResult(t *testing.T) {
	env := es.Envelope{StreamID: es.StreamID{Type: "counter", ID: "main"}, Version: 3}
	base := &appendStub{result: es.AppendResult{
		FromVersion: 2,
		ToVersion:   3,
		Envelopes:   []es.Envelope{env},
	}}

	var calls int
	var got es.AppendResult
	wrapped := es.WithAfterAppend(base, func(_ context.Context, res es.AppendResult) {
		calls++
		got = res
	})

	res, err := wrapped.Append(context.Background(), es.AppendParams{ExpectedVersion: 2})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if calls != 1 {
		t.Fatalf("hook fired %d times, want exactly once", calls)
	}
	if got.FromVersion != 2 || got.ToVersion != 3 {
		t.Fatalf("hook saw FromVersion=%d ToVersion=%d, want 2/3", got.FromVersion, got.ToVersion)
	}
	if len(got.Envelopes) != 1 || !reflect.DeepEqual(got.Envelopes[0], env) {
		t.Fatalf("hook saw envelopes %+v, want the single appended envelope at position 0", got.Envelopes)
	}
	if base.appends != 1 {
		t.Fatalf("base.Append called %d times, want 1", base.appends)
	}
	if res.FromVersion != base.result.FromVersion || res.ToVersion != base.result.ToVersion || !reflect.DeepEqual(res.Envelopes, base.result.Envelopes) {
		t.Fatalf("Append returned %+v, want the stub's result %+v", res, base.result)
	}
}

func TestWithAfterAppendDoesNotFireOnError(t *testing.T) {
	cases := map[string]error{
		"conflict": es.ErrConflict,
		"generic":  errors.New("boom"),
	}
	for name, err := range cases {
		t.Run(name, func(t *testing.T) {
			base := &appendStub{err: err}
			fired := false
			wrapped := es.WithAfterAppend(base, func(context.Context, es.AppendResult) { fired = true })

			if _, aerr := wrapped.Append(context.Background(), es.AppendParams{}); aerr != err {
				t.Fatalf("append error = %v, want %v", aerr, err)
			}
			if fired {
				t.Fatal("hook fired on a failed append, want it uncalled")
			}
		})
	}
}

func TestWithAfterAppendRecoversPanic(t *testing.T) {
	base := &appendStub{result: es.AppendResult{FromVersion: 1, ToVersion: 1}}
	wrapped := es.WithAfterAppend(base, func(context.Context, es.AppendResult) {
		panic("hint blew up")
	})

	res, err := wrapped.Append(context.Background(), es.AppendParams{})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if res.ToVersion != 1 {
		t.Fatalf("res.ToVersion = %d, want the committed result", res.ToVersion)
	}
	if base.appends != 1 {
		t.Fatalf("base.Append called %d times, want 1", base.appends)
	}
}

func TestWithAfterAppendClaimOnlyAppend(t *testing.T) {
	base := &appendStub{result: es.AppendResult{}} // zero envelopes: a claim-only append
	fired := 0
	var saw int
	wrapped := es.WithAfterAppend(base, func(_ context.Context, res es.AppendResult) {
		fired++
		saw = len(res.Envelopes) // must not panic: the hook cannot index Envelopes[0]
	})

	if _, err := wrapped.Append(context.Background(), es.AppendParams{}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if fired != 1 {
		t.Fatalf("hook fired %d times, want exactly once", fired)
	}
	if saw != 0 {
		t.Fatalf("hook saw %d envelopes, want 0 for a claim-only append", saw)
	}
}

func TestWithAfterAppendNilFnReturnsStore(t *testing.T) {
	base := &appendStub{}
	if got := es.WithAfterAppend(base, nil); got != es.Store(base) {
		t.Fatalf("WithAfterAppend(base, nil) = %v, want base itself", got)
	}
}

func TestWithAfterAppendPassesOtherMethodsThrough(t *testing.T) {
	base := &appendStub{}
	wrapped := es.WithAfterAppend(base, func(context.Context, es.AppendResult) {})

	if _, err := wrapped.ReadStream(context.Background(), es.StreamID{Type: "counter", ID: "main"}, 0, 0); err != nil {
		t.Fatalf("read: %v", err)
	}
	if base.reads != 1 {
		t.Fatalf("ReadStream delegated %d times, want exactly once", base.reads)
	}
}

func TestWithAfterAppendHasNoAppendTx(t *testing.T) {
	base := &appendStub{}
	wrapped := es.WithAfterAppend(base, func(context.Context, es.AppendResult) {})

	if m, ok := reflect.TypeOf(wrapped).MethodByName("AppendTx"); ok {
		t.Fatalf("wrapper exposes AppendTx (%s); AppendTx is not in es.Store and must not be reachable", m.Name)
	}
}
