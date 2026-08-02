// Package aggregate is the write-side runtime: it loads a stream, folds
// its events into state via the Decider, runs a command, and appends the
// resulting events under optimistic concurrency.
//
// It is generic over the aggregate's state (S), command sum type (C),
// and event sum type (E). One Runtime instance serves every stream of a
// given aggregate type.
package aggregate

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/es-lite/es"
)

// Runtime handles commands for one aggregate type.
type Runtime[S, C, E any] struct {
	store    es.Store
	decider  es.Decider[S, C, E]
	codec    es.Codec[E]
	upcaster es.Upcaster[E] // optional; applied after decode on the read path
}

// NewRuntime wires a Runtime against a Store, a Decider, and the event
// Codec (generated per aggregate; hand-written for now).
func NewRuntime[S, C, E any](store es.Store, decider es.Decider[S, C, E], codec es.Codec[E]) *Runtime[S, C, E] {
	return &Runtime[S, C, E]{store: store, decider: decider, codec: codec}
}

// WithUpcaster installs an Upcaster applied to every event after decode
// during Load/replay — the hook that upgrades events stored under an older
// (es.v1.schema_version) to the current shape. Returns the runtime for
// chaining.
func (r *Runtime[S, C, E]) WithUpcaster(u es.Upcaster[E]) *Runtime[S, C, E] {
	r.upcaster = u
	return r
}

// Result is what Handle returns: the post-command state, the events it
// produced, and the version range they occupy.
type Result[S, E any] struct {
	State       S
	Events      []E
	FromVersion uint64
	ToVersion   uint64
	Envelopes   []es.Envelope
}

// Handle loads the stream, applies cmd via Decider.Decide, and appends
// the resulting events. On an optimistic-concurrency clash it returns
// es.ErrConflict — the caller reloads and retries. The command's audit
// metadata is taken from meta; missing CommandID/OccurredAt are filled
// in here (the runtime, unlike the Decider, may touch the clock and
// generate IDs).
func (r *Runtime[S, C, E]) Handle(ctx context.Context, sid es.StreamID, cmd C, meta es.Meta) (Result[S, E], error) {
	var zero Result[S, E]

	state, version, err := r.load(ctx, sid, 0, time.Time{})
	if err != nil {
		return zero, err
	}

	if r.decider.IsTerminal != nil && r.decider.IsTerminal(state) {
		return zero, fmt.Errorf("%w: %s", es.ErrTerminal, sid)
	}

	events, constraints, err := r.decider.Decide(state, cmd)
	if err != nil {
		return zero, err // domain rejection — surfaced verbatim to the caller
	}
	if len(events) == 0 {
		// A no-op command: nothing to append. Return current state.
		return Result[S, E]{State: state, FromVersion: version, ToVersion: version}, nil
	}

	encoded := make([]es.EventData, len(events))
	for i, ev := range events {
		enc, err := r.codec.Encode(ev)
		if err != nil {
			return zero, fmt.Errorf("encode event %d: %w", i, err)
		}
		encoded[i] = es.EventData{
			TypeURL:       enc.TypeURL,
			SchemaVersion: enc.SchemaVersion,
			Payload:       enc.Payload,
		}
	}

	// Fill audit defaults (the runtime, unlike the Decider, may read the
	// clock and mint IDs). CorrelationID defaults to the command id so a
	// single command still forms a one-event correlation group.
	if meta.CommandID == uuid.Nil {
		meta.CommandID = newV7()
	}
	if meta.CorrelationID == uuid.Nil {
		meta.CorrelationID = meta.CommandID
	}
	if meta.OccurredAt.IsZero() {
		meta.OccurredAt = time.Now().UTC()
	}

	res, err := r.store.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: version,
		Events:          encoded,
		Constraints:     constraints,
		CommandID:       meta.CommandID,
		CorrelationID:   meta.CorrelationID,
		CausationID:     meta.CausationID,
		Actor:           meta.Actor,
		OccurredAt:      meta.OccurredAt,
	})
	if err != nil {
		return zero, err // includes es.ErrConflict
	}

	// Fold the just-appended events forward so the caller gets
	// read-your-writes state without re-reading the store.
	for _, ev := range events {
		state = r.decider.Evolve(state, ev)
	}

	return Result[S, E]{
		State:       state,
		Events:      events,
		FromVersion: res.FromVersion,
		ToVersion:   res.ToVersion,
		Envelopes:   res.Envelopes,
	}, nil
}

// Load returns the current folded state of a stream and its version.
func (r *Runtime[S, C, E]) Load(ctx context.Context, sid es.StreamID) (state S, version uint64, err error) {
	return r.load(ctx, sid, 0, time.Time{})
}

// LoadAsOfVersion returns the state as it stood after exactly version v
// (time-travel by version). v=0 returns Initial().
func (r *Runtime[S, C, E]) LoadAsOfVersion(ctx context.Context, sid es.StreamID, v uint64) (S, error) {
	state, _, err := r.load(ctx, sid, v, time.Time{})
	return state, err
}

// LoadAsOfTime returns the state as it stood at wall-clock time t, based
// on each event's RecordedAt (time-travel by time).
func (r *Runtime[S, C, E]) LoadAsOfTime(ctx context.Context, sid es.StreamID, t time.Time) (S, error) {
	state, _, err := r.load(ctx, sid, 0, t)
	return state, err
}

// load folds a stream into state. Exactly one of upToVersion / asOf
// bounds the fold: upToVersion>0 caps by version, a non-zero asOf caps
// by RecordedAt, and neither means "the whole stream".
func (r *Runtime[S, C, E]) load(ctx context.Context, sid es.StreamID, upToVersion uint64, asOf time.Time) (S, uint64, error) {
	state := r.decider.Initial()

	var (
		envs []es.Envelope
		err  error
	)
	switch {
	case !asOf.IsZero():
		envs, err = r.store.ReadStreamAsOf(ctx, sid, asOf)
	default:
		envs, err = r.store.ReadStream(ctx, sid, 0, upToVersion)
	}
	if err != nil && !errors.Is(err, es.ErrStreamNotFound) {
		return state, 0, err
	}

	var version uint64
	for _, env := range envs {
		ev, err := r.codec.Decode(es.EncodedEvent{
			TypeURL:       env.TypeURL,
			SchemaVersion: env.SchemaVersion,
			Payload:       env.Payload,
		})
		if err != nil {
			return state, 0, fmt.Errorf("decode event %s v%d: %w", sid, env.Version, err)
		}
		if r.upcaster != nil {
			ev, err = r.upcaster.Upcast(env.TypeURL, env.SchemaVersion, ev)
			if err != nil {
				return state, 0, fmt.Errorf("upcast event %s v%d: %w", sid, env.Version, err)
			}
		}
		state = r.decider.Evolve(state, ev)
		version = env.Version
	}
	return state, version, nil
}

// newV7 generates a UUIDv7 (time-ordered). Falls back to v4 only if the
// platform clock read fails, which effectively never happens.
func newV7() uuid.UUID {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.New()
	}
	return id
}
