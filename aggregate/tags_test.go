package aggregate_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
)

type tagState struct{}
type tagCommand struct{}
type tagEvent struct{ id string }

type tagCodec struct{}

func (tagCodec) Encode(e tagEvent) (es.EncodedEvent, error) {
	return es.EncodedEvent{TypeURL: "thing.v1.Tagged", SchemaVersion: 1, Payload: []byte(e.id)}, nil
}
func (tagCodec) Decode(es.EncodedEvent) (tagEvent, error) { return tagEvent{}, nil }

// tagRecordingStore records the AppendParams the runtime builds, then
// delegates to the real store so the append still exercises the normal path.
type tagRecordingStore struct {
	es.Store
	last es.AppendParams
}

func (s *tagRecordingStore) Append(ctx context.Context, p es.AppendParams) (es.AppendResult, error) {
	s.last = p
	return s.Store.Append(ctx, p)
}

func TestRuntimeCopiesTags(t *testing.T) {
	ctx := context.Background()
	store := &tagRecordingStore{Store: openStore(t)}
	sid, err := es.NewStreamID("thing", "tags")
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}

	n := 0
	decider := es.Decider[tagState, tagCommand, tagEvent]{
		Initial: func() tagState { return tagState{} },
		Decide: func(tagState, tagCommand) ([]tagEvent, []es.ConstraintOp, error) {
			return []tagEvent{{id: "1"}, {id: "2"}}, nil, nil
		},
		Evolve: func(s tagState, _ tagEvent) tagState { return s },
		Tags: func(tagEvent) map[string]string {
			n++
			return map[string]string{"flowdoc.block": fmt.Sprintf("b%d", n)}
		},
	}
	rt := aggregate.NewRuntime(store, decider, tagCodec{})

	if _, err := rt.Handle(ctx, sid, tagCommand{}, es.Meta{
		Tags: map[string]string{"flowdoc.space": "s1"},
	}); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if want := map[string]string{"flowdoc.space": "s1"}; !reflect.DeepEqual(store.last.Tags, want) {
		t.Fatalf("AppendParams.Tags = %#v, want %#v", store.last.Tags, want)
	}
	got := make([]map[string]string, len(store.last.Events))
	for i := range store.last.Events {
		got[i] = store.last.Events[i].Tags
	}
	want := []map[string]string{
		{"flowdoc.block": "b1"},
		{"flowdoc.block": "b2"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("EventData.Tags = %#v, want %#v", got, want)
	}

	// A decider without a Tags function leaves the per-event tags nil.
	store.last = es.AppendParams{}
	plain := aggregate.NewRuntime(store, es.Decider[tagState, tagCommand, tagEvent]{
		Initial: func() tagState { return tagState{} },
		Decide: func(tagState, tagCommand) ([]tagEvent, []es.ConstraintOp, error) {
			return []tagEvent{{id: "3"}}, nil, nil
		},
		Evolve: func(s tagState, _ tagEvent) tagState { return s },
	}, tagCodec{})
	if _, err := plain.Handle(ctx, sid, tagCommand{}, es.Meta{}); err != nil {
		t.Fatalf("Handle without Tags: %v", err)
	}
	if store.last.Tags != nil {
		t.Fatalf("AppendParams.Tags = %#v, want nil", store.last.Tags)
	}
	if store.last.Events[0].Tags != nil {
		t.Fatalf("EventData.Tags = %#v, want nil", store.last.Events[0].Tags)
	}
}
