package aggregate_test

import (
	"context"
	"testing"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/examples/counter"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
)

// TestAfterAppendHookDoesNotFireWhenWithAppendReplacesTheAppend pins the
// ADR 0039 boundary: aggregate.WithAppend replaces store.Append for the
// call (aggregate/runtime.go:398-401), so the es.WithAfterAppend decorator
// is bypassed and the hint must not fire. The caller owns that transaction
// and wakes after its own commit. A following plain Handle goes through the
// decorator and fires the hook exactly once.
func TestAfterAppendHookDoesNotFireWhenWithAppendReplacesTheAppend(t *testing.T) {
	ctx := context.Background()
	base := openStore(t)
	sid, _ := es.NewStreamID("counter", "hooked")

	fired := 0
	wrapped := es.WithAfterAppend(base, func(context.Context, es.AppendResult) { fired++ })
	rt := aggregate.NewRuntime(wrapped, counter.Decider, counterv1.EventCodec{})

	// Caller-owned transaction: WithAppend swaps in base.Append directly,
	// so the decorator (and its hook) is not on the call path.
	if _, err := rt.Handle(ctx, sid,
		counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 1}),
		es.Meta{}, aggregate.WithAppend(base.Append)); err != nil {
		t.Fatalf("init via WithAppend: %v", err)
	}
	if fired != 0 {
		t.Fatalf("hook fired %d times under WithAppend, want 0 — WithAppend replaces store.Append and bypasses the decorator", fired)
	}

	// A plain Handle appends through the wrapped store: the hook fires.
	if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Increment{By: 1}), es.Meta{}); err != nil {
		t.Fatalf("increment: %v", err)
	}
	if fired != 1 {
		t.Fatalf("hook fired %d times after plain Handle, want exactly once", fired)
	}
}
