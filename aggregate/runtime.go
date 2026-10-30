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
	"math/rand/v2"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/snapshot"
)

// Runtime handles commands for one aggregate type.
type Runtime[S, C, E any] struct {
	store    es.Store
	decider  es.Decider[S, C, E]
	codec    es.Codec[E]
	upcaster es.Upcaster[E] // optional; applied after decode on the read path

	// Optional snapshot cache (ADR 0007/0009). When set, Load seeds from a
	// cached state and folds only the tail, and Handle writes the state
	// through after appending. foldVersion invalidates snapshots on a
	// decider/upcaster/state-shape change.
	cache       snapshot.Cache
	stateCodec  es.StateCodec[S]
	foldVersion uint32

	// Bounded conflict retry (ADR 0032). retries is the number of ADDITIONAL
	// Handle attempts after an es.ErrConflict; 0 (the default) surfaces the
	// first conflict to the caller. backoff maps the 1-based retry number to
	// the pause before it.
	retries int
	backoff func(attempt int) time.Duration

	// verifyChain makes load recompute the hash chain of every envelope it
	// folds (ADR 0029 §3). Off by default: it doubles hashing on the read
	// path, and a gate can verify once per fetch instead.
	verifyChain bool
}

// NewRuntime wires a Runtime against a Store, a Decider, and the generated
// per-aggregate event Codec.
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

// WithSnapshots enables the snapshot cache: Load seeds from a cached state and
// folds only the tail, and Handle writes the new state through after append.
// stateCodec serializes S; foldVersion is a monotonic counter you bump when a
// decider/upcaster/state-shape change makes old snapshots wrong (they are then
// ignored and refolded — snapshots are a pure cache). Returns the runtime for
// chaining.
func (r *Runtime[S, C, E]) WithSnapshots(cache snapshot.Cache, stateCodec es.StateCodec[S], foldVersion uint32) *Runtime[S, C, E] {
	r.cache = cache
	r.stateCodec = stateCodec
	r.foldVersion = foldVersion
	return r
}

// WithConflictRetry makes Handle repeat load → IsTerminal → Decide → Append up
// to n additional times when Append returns es.ErrConflict, sleeping
// backoff(attempt) (attempt is 1-based; ctx cancellation cuts the sleep short)
// between attempts. backoff == nil selects DefaultBackoff. Only ErrConflict is
// retried: ErrTerminal, ErrConstraintViolated, decider rejections and store
// errors return immediately. The final error still matches es.ErrConflict with
// errors.Is. CommandID is minted once, so every attempt carries the same id.
//
// The default is no retry: a human-issued command should surface the conflict
// with the current state rather than silently win. Machine callers (reaction
// runners, generated command services) set it explicitly (ADR 0032).
// Returns the runtime for chaining.
func (r *Runtime[S, C, E]) WithConflictRetry(n int, backoff func(attempt int) time.Duration) *Runtime[S, C, E] {
	if n < 0 {
		n = 0
	}
	if backoff == nil {
		backoff = DefaultBackoff
	}
	r.retries = n
	r.backoff = backoff
	return r
}

// ConflictRetries reports the retry budget set by WithConflictRetry (0 when
// unset), so a caller such as reaction.RuntimeDispatcher can add its own
// only when the runtime has none.
func (r *Runtime[S, C, E]) ConflictRetries() int { return r.retries }

// WithChainVerification makes every load recompute the per-stream hash chain
// over the envelopes it folds and fail with es.ErrChainBroken on a mismatch
// (ADR 0029 §3). Returns the runtime for chaining.
func (r *Runtime[S, C, E]) WithChainVerification() *Runtime[S, C, E] {
	r.verifyChain = true
	return r
}

// DefaultBackoff is the full-jitter exponential schedule WithConflictRetry
// uses when given a nil backoff: a uniform random pause in [0, base] where
// base doubles from 10ms per attempt and is capped at 500ms.
func DefaultBackoff(attempt int) time.Duration {
	const (
		start   = 10 * time.Millisecond
		ceiling = 500 * time.Millisecond
	)
	if attempt < 1 {
		attempt = 1
	}
	base := start
	for i := 1; i < attempt && base < ceiling; i++ {
		base *= 2
	}
	if base > ceiling {
		base = ceiling
	}
	return time.Duration(rand.Int64N(int64(base) + 1))
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
// es.ErrConflict — the caller reloads and retries, unless WithConflictRetry
// is set, in which case Handle retries a bounded number of times itself.
// The command's audit metadata is taken from meta; missing
// CommandID/OccurredAt are filled in here (the runtime, unlike the Decider,
// may touch the clock and generate IDs). A command implementing es.DecidedAt
// supplies OccurredAt itself (ADR 0032): a zero meta.OccurredAt is filled
// from it, a differing non-zero one is rejected with es.ErrDecidedAtMismatch,
// and an unset decided_at is rejected with es.ErrDecidedAtMissing.
func (r *Runtime[S, C, E]) Handle(ctx context.Context, sid es.StreamID, cmd C, meta es.Meta) (Result[S, E], error) {
	return r.HandleAt(ctx, sid, cmd, meta, 0)
}

// HandleAt is Handle with a caller-asserted stream version (ADR 0031): when
// expected is non-zero and the stream's current version differs, it returns
// an *es.ConflictError carrying both versions without deciding or
// appending. Unlike a write race, that conflict is never retried by
// WithConflictRetry — the caller's assertion is stale, and only the caller
// can refresh it. expected == 0 means "no assertion" (plain Handle).
func (r *Runtime[S, C, E]) HandleAt(ctx context.Context, sid es.StreamID, cmd C, meta es.Meta, expected uint64) (Result[S, E], error) {
	var zero Result[S, E]

	// Domain time from the command (ADR 0032). Checked before any I/O so a
	// disagreement never costs a load and never appends.
	if d, ok := any(cmd).(es.DecidedAt); ok {
		decided := d.DecisionTime()
		switch {
		case decided.IsZero():
			return zero, fmt.Errorf("%w: %T", es.ErrDecidedAtMissing, cmd)
		case meta.OccurredAt.IsZero():
			meta.OccurredAt = decided
		case !meta.OccurredAt.Equal(decided):
			return zero, fmt.Errorf("%w: meta %s, command %s",
				es.ErrDecidedAtMismatch, meta.OccurredAt.UTC().Format(time.RFC3339Nano), decided.UTC().Format(time.RFC3339Nano))
		}
	}

	// Fill audit defaults once, outside the retry loop, so every attempt
	// carries the same command id and domain time. CorrelationID defaults
	// to the command id so a single command still forms a one-event
	// correlation group.
	if meta.CommandID == uuid.Nil {
		meta.CommandID = newV7()
	}
	if meta.CorrelationID == uuid.Nil {
		meta.CorrelationID = meta.CommandID
	}
	if meta.OccurredAt.IsZero() {
		meta.OccurredAt = time.Now().UTC()
	}

	for attempt := 0; ; attempt++ {
		res, err := r.handleOnce(ctx, sid, cmd, meta, expected)
		var stale *es.ConflictError
		if err == nil || !errors.Is(err, es.ErrConflict) || errors.As(err, &stale) || attempt >= r.retries {
			if err != nil && attempt > 0 {
				return zero, fmt.Errorf("aggregate: conflict retry exhausted after %d attempts: %w", attempt+1, err)
			}
			return res, err
		}
		if err := r.sleep(ctx, r.backoff(attempt+1)); err != nil {
			return zero, err
		}
	}
}

// sleep pauses for d, returning early with ctx.Err() on cancellation.
func (r *Runtime[S, C, E]) sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// handleOnce is a single load → IsTerminal → Decide → Append pass. Decide is
// pure and load re-reads the stream, so re-running it after a conflict is
// safe by construction.
func (r *Runtime[S, C, E]) handleOnce(ctx context.Context, sid es.StreamID, cmd C, meta es.Meta, expected uint64) (Result[S, E], error) {
	var zero Result[S, E]

	state, version, err := r.load(ctx, sid, 0, time.Time{})
	if err != nil {
		return zero, err
	}
	if expected != 0 && version != expected {
		return zero, &es.ConflictError{StreamID: sid, Expected: expected, Current: version}
	}

	if r.decider.IsTerminal != nil && r.decider.IsTerminal(state) {
		return zero, fmt.Errorf("%w: %s", es.ErrTerminal, sid)
	}

	events, constraints, err := r.decider.Decide(state, cmd)
	if err != nil {
		return zero, err // domain rejection — surfaced verbatim to the caller
	}
	if len(events) == 0 && len(constraints) == 0 {
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

	// Write the new state through to the snapshot cache (best-effort: a
	// failure only costs a longer fold next time — snapshots are a pure cache).
	if r.cache != nil {
		if b, encErr := r.stateCodec.Encode(state); encErr == nil {
			_ = r.cache.Save(ctx, sid.Canonical(), snapshot.Snapshot{
				Version:     res.ToVersion,
				FoldVersion: r.foldVersion,
				RecordedAt:  time.Now().UTC(),
				State:       b,
			})
		}
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
// (time-travel by version). v=0 returns Initial(); v beyond the stream's
// current version returns es.ErrVersionOutOfRange.
func (r *Runtime[S, C, E]) LoadAsOfVersion(ctx context.Context, sid es.StreamID, v uint64) (S, error) {
	if err := sid.Validate(); err != nil {
		var zero S
		return zero, err
	}
	if v == 0 {
		return r.decider.Initial(), nil
	}
	state, _, err := r.load(ctx, sid, v, time.Time{})
	return state, err
}

// LoadAsOfTime returns the state as it stood at wall-clock time t, based
// on each event's RecordedAt (time-travel by time).
func (r *Runtime[S, C, E]) LoadAsOfTime(ctx context.Context, sid es.StreamID, t time.Time) (S, error) {
	state, _, err := r.load(ctx, sid, 0, t)
	return state, err
}

// LoadStale is a bounded-staleness read (ADR 0009): if a snapshot exists,
// matches the fold version, and is within maxAge, it returns the cached state
// directly WITHOUT reading the log — zero store round-trips. Otherwise it falls
// back to a strong Load (snapshot + tail fold). The bool reports whether the
// fast, cache-only path was taken. maxAge<=0 accepts any snapshot age.
//
// Safe because snapshots are a pure cache; use it for reads that tolerate a
// little staleness (dashboards, list views) to skip the store entirely.
func (r *Runtime[S, C, E]) LoadStale(ctx context.Context, sid es.StreamID, maxAge time.Duration) (state S, version uint64, fresh bool, err error) {
	if r.cache != nil {
		if snap, ok, serr := r.cache.Load(ctx, sid.Canonical()); serr == nil && ok && snap.FoldVersion == r.foldVersion {
			if maxAge <= 0 || time.Since(snap.RecordedAt) <= maxAge {
				if s, derr := r.stateCodec.Decode(snap.State); derr == nil {
					return s, snap.Version, true, nil
				}
			}
		}
	}
	state, version, err = r.Load(ctx, sid)
	return state, version, false, err
}

// load folds a stream into state. Exactly one of upToVersion / asOf
// bounds the fold: upToVersion>0 caps by version, a non-zero asOf caps
// by RecordedAt, and neither means "the whole stream".
func (r *Runtime[S, C, E]) load(ctx context.Context, sid es.StreamID, upToVersion uint64, asOf time.Time) (S, uint64, error) {
	state := r.decider.Initial()

	// Snapshot seed — only for current (non-time-travel) loads. Time-travel
	// must never start from a snapshot newer than the target (ADR 0007). Any
	// miss/mismatch/error falls through to a full fold (pure cache).
	var from uint64
	if r.cache != nil && upToVersion == 0 && asOf.IsZero() {
		if snap, ok, serr := r.cache.Load(ctx, sid.Canonical()); serr == nil && ok && snap.FoldVersion == r.foldVersion {
			if s, derr := r.stateCodec.Decode(snap.State); derr == nil {
				state = s
				from = snap.Version
			}
		}
	}

	var (
		envs []es.Envelope
		err  error
	)
	switch {
	case !asOf.IsZero():
		envs, err = r.store.ReadStreamAsOf(ctx, sid, asOf)
	default:
		envs, err = r.store.ReadStream(ctx, sid, from, upToVersion)
	}
	if err != nil && !errors.Is(err, es.ErrStreamNotFound) {
		return state, 0, err
	}

	var checker *es.ChainChecker
	if r.verifyChain {
		checker = es.NewChainChecker(sid)
	}
	version := from
	for _, env := range envs {
		if checker != nil {
			if err := checker.Next(env); err != nil {
				return state, 0, err
			}
		}
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
	if upToVersion > 0 && version != upToVersion {
		return state, version, fmt.Errorf("%w: stream %s has version %d, want %d",
			es.ErrVersionOutOfRange, sid, version, upToVersion)
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
