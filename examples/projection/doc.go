// Package projection is the worked example for ADR 0030: a read model
// (CounterView), a metadata-only index (CounterActivity) and a reaction
// (ResetOnClosed) declared as message options in
// examples/counter/proto/counterview/v1/view.proto, with the handler code
// hand-written here and everything else — decoding, state fetching as of
// the event's version, marker handling, rebuild, the issue/execute loop —
// generated into gen/counterview/v1 by protoc-gen-es-lite.
//
// The tests run the generated runners over projection.LogFeed (the log
// itself) with the embedded aggregate.Runtime as the state loader, so a
// projector is exercised end to end on SQLite with no NATS. The JetStream
// path (natsjs.NewFeed with the generated filter subjects) is covered under
// NATS_URL.
package projection

import (
	"context"
	"sync"

	"github.com/laenenai/es-lite/es"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
	counterviewv1 "github.com/laenenai/es-lite/gen/counterview/v1"
)

// Row is one CounterView row — the plain-struct shape of the
// counterview.v1.CounterView message, as a read-model table would hold it.
type Row struct {
	CounterID string
	Count     int64
	Closed    bool
	Version   uint64
}

// Proto renders the row as the API message.
func (r Row) Proto() *counterviewv1.CounterView {
	return &counterviewv1.CounterView{CounterId: r.CounterID, Count: r.Count, Closed: r.Closed, Version: r.Version}
}

// View is an in-memory CounterView read model: one row per counter.
type View struct {
	mu   sync.Mutex
	rows map[string]Row
}

// NewView returns an empty read model.
func NewView() *View { return &View{rows: map[string]Row{}} }

var _ counterviewv1.CounterViewHandler = (*View)(nil)

// put records the state the runner fetched as of env.Version — the handler
// never computes the count itself, so a rebuild cannot drift from live.
func (v *View) put(env es.Envelope, state *counterv1.Counter) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.rows[env.StreamID.ID] = Row{
		CounterID: env.StreamID.ID,
		Count:     state.GetCount(),
		Closed:    state.GetClosed(),
		Version:   env.Version,
	}
	return nil
}

func (v *View) OnInitialized(_ context.Context, env es.Envelope, _ *counterv1.Initialized, state *counterv1.Counter) error {
	return v.put(env, state)
}
func (v *View) OnIncremented(_ context.Context, env es.Envelope, _ *counterv1.Incremented, state *counterv1.Counter) error {
	return v.put(env, state)
}
func (v *View) OnDecremented(_ context.Context, env es.Envelope, _ *counterv1.Decremented, state *counterv1.Counter) error {
	return v.put(env, state)
}
func (v *View) OnClosed(_ context.Context, env es.Envelope, _ *counterv1.Closed, state *counterv1.Counter) error {
	return v.put(env, state)
}

// Get returns one row.
func (v *View) Get(id string) (Row, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	r, ok := v.rows[id]
	return r, ok
}

// Snapshot returns every row keyed by counter id, for equality checks.
func (v *View) Snapshot() map[string]Row {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make(map[string]Row, len(v.rows))
	for id, r := range v.rows {
		out[id] = r
	}
	return out
}

// Activity is the metadata-only CounterActivity index: events per counter.
type Activity struct {
	mu     sync.Mutex
	events map[string]uint64
}

// NewActivity returns an empty index.
func NewActivity() *Activity { return &Activity{events: map[string]uint64{}} }

var _ counterviewv1.CounterActivityHandler = (*Activity)(nil)

func (a *Activity) bump(env es.Envelope) error {
	a.mu.Lock()
	a.events[env.StreamID.ID]++
	a.mu.Unlock()
	return nil
}

func (a *Activity) OnIncremented(_ context.Context, env es.Envelope) error { return a.bump(env) }
func (a *Activity) OnDecremented(_ context.Context, env es.Envelope) error { return a.bump(env) }

// Events returns the count for one counter.
func (a *Activity) Events(id string) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.events[id]
}

// Reset is the ResetOnClosed reaction: when a counter closes, open its
// successor "<id>-next" with the same bounds, starting at the minimum. Init
// rejects a second initialisation, which the test uses to show that a
// re-issued command on a stream that already took it does not break the
// runner (es.ErrTerminal is "done"; a decider rejection is surfaced).
type Reset struct{}

var _ counterviewv1.ResetOnClosedHandler = Reset{}

func (Reset) OnClosed(_ context.Context, env es.Envelope, _ *counterv1.Closed, state *counterv1.Counter, issue *counterviewv1.ResetOnClosedIssuer) error {
	next, err := es.NewStreamID(env.StreamID.Type, env.StreamID.ID+"-next")
	if err != nil {
		return err
	}
	issue.Init(next, &counterv1.Init{Min: state.GetMin(), Max: state.GetMax(), Initial: state.GetMin()})
	return nil
}
