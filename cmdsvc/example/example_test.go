// Package example_test spins up the generated CounterCommandService handler
// in-process with connect-go over httptest and exercises it as a client
// would (ADR 0031): happy path with WriteResult + state, the acting
// principal overwritten even when the client sets it, a stale
// expected_version answered with failed_precondition + VersionConflict
// (current version and state), a protovalidate failure as invalid_argument
// with the violations detail, and a terminal stream.
package example_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/cmdsvc"
	countersvcv1 "github.com/laenenai/es-lite/cmdsvc/example/gen/countersvc/v1"
	"github.com/laenenai/es-lite/cmdsvc/example/gen/countersvc/v1/countersvcv1connect"
	"github.com/laenenai/es-lite/cmdsvc/example/gen/countersvc/v1/countersvcv1es"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/examples/counter"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
	"github.com/laenenai/es-lite/sqlite"
)

// identity is the app's IdentitySource: a fixed authenticated subject.
type identity struct{ subject string }

func (i identity) Principal(context.Context) (*counterv1.Principal, es.Actor, error) {
	return &counterv1.Principal{Subject: i.subject}, es.Actor{Type: "user", ID: i.subject}, nil
}

// authorizer allows everything except Close by anyone but "owner".
type authorizer struct{}

func (authorizer) AuthorizeInit(context.Context, es.StreamID, *counterv1.Init) error { return nil }
func (authorizer) AuthorizeIncrement(context.Context, es.StreamID, *counterv1.Increment) error {
	return nil
}
func (authorizer) AuthorizeClose(_ context.Context, _ es.StreamID, cmd *counterv1.Close) error {
	return nil
}

type harness struct {
	client countersvcv1connect.CounterCommandServiceClient
	rt     *aggregate.Runtime[*counterv1.Counter, counterv1.Command, counterv1.Event]
	st     *sqlite.Store
}

func (h harness) store() es.Store { return h.st }

func newHarness(t *testing.T, subject string) harness {
	t.Helper()
	store, err := sqlite.Open(context.Background(), "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{})
	handler := countersvcv1es.NewCounterCommandServiceHandler(countersvcv1es.CounterCommandServiceDeps{
		Runtime:   rt,
		Authorize: authorizer{},
		Identity:  identity{subject: subject},
		Errors: func(err error) *connect.Error {
			if errors.Is(err, counter.ErrOutOfRange) {
				return connect.NewError(connect.CodeOutOfRange, err)
			}
			return nil
		},
	})
	mux := http.NewServeMux()
	mux.Handle(countersvcv1connect.NewCounterCommandServiceHandler(handler))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return harness{client: countersvcv1connect.NewCounterCommandServiceClient(srv.Client(), srv.URL), rt: rt, st: store}
}

func TestCommandServiceHappyPathAndPrincipal(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "alice")

	res, err := h.client.Init(ctx, connect.NewRequest(&countersvcv1.InitRequest{
		CounterId: "main", Command: &counterv1.Init{Min: 0, Max: 100, Initial: 5},
	}))
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	if r := res.Msg.GetResult(); r.GetStreamId() != "counter:main" || r.GetVersion() != 1 || r.GetGlobalPosition() != 1 {
		t.Fatalf("write result = %+v", r)
	}
	if res.Msg.GetState().GetCount() != 5 {
		t.Fatalf("state = %+v", res.Msg.GetState())
	}

	// The client claims to be "mallory"; the handler overwrites the
	// principal from the identity source and stamps the actor on the
	// envelope. Correlation-Id and Idempotency-Key ride the headers.
	req := connect.NewRequest(&countersvcv1.IncrementRequest{
		CounterId: "main",
		Command:   &counterv1.Increment{By: 10, Actor: &counterv1.Principal{Subject: "mallory"}},
	})
	req.Header().Set(cmdsvc.HdrCorrelationID, "0192f3a0-0000-7000-8000-0000000000c0")
	req.Header().Set(cmdsvc.HdrIdempotencyKey, "order-42")
	inc, err := h.client.Increment(ctx, req)
	if err != nil {
		t.Fatalf("increment: %v", err)
	}
	if inc.Msg.GetState().GetCount() != 15 || inc.Msg.GetResult().GetVersion() != 2 {
		t.Fatalf("increment result = %+v state=%+v", inc.Msg.GetResult(), inc.Msg.GetState())
	}
	sid, _ := es.NewStreamID("counter", "main")
	rows, err := storeOf(h).ReadStream(ctx, sid, 1, 2)
	if err != nil || len(rows) != 1 {
		t.Fatalf("read: %v", err)
	}
	if rows[0].Actor != (es.Actor{Type: "user", ID: "alice"}) {
		t.Fatalf("actor = %+v, want alice (client said mallory)", rows[0].Actor)
	}
	if rows[0].CorrelationID.String() != "0192f3a0-0000-7000-8000-0000000000c0" {
		t.Fatalf("correlation id = %s", rows[0].CorrelationID)
	}
	// A second request with the same Idempotency-Key gets the same CommandID.
	req2 := connect.NewRequest(&countersvcv1.IncrementRequest{CounterId: "main", Command: &counterv1.Increment{By: 1}})
	req2.Header().Set(cmdsvc.HdrIdempotencyKey, "order-42")
	if _, err := h.client.Increment(ctx, req2); err != nil {
		t.Fatal(err)
	}
	rows, _ = storeOf(h).ReadStream(ctx, sid, 1, 3)
	if rows[0].CommandID != rows[1].CommandID || rows[0].CommandID.String() == "00000000-0000-0000-0000-000000000000" {
		t.Fatalf("command ids differ for the same idempotency key: %s vs %s", rows[0].CommandID, rows[1].CommandID)
	}
}

func TestCommandServicePrincipalReachesTheDecider(t *testing.T) {
	// Prove the overwrite happens before Decide: a decider probe records
	// the principal it saw.
	ctx := context.Background()
	store, err := sqlite.Open(ctx, "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	var seen string
	probe := counter.Decider
	inner := counter.Decider.Decide
	probe.Decide = func(s *counterv1.Counter, c counterv1.Command) ([]counterv1.Event, []es.ConstraintOp, error) {
		if inc, ok := c.(*counterv1.Increment); ok {
			seen = inc.GetActor().GetSubject()
		}
		return inner(s, c)
	}
	rt := aggregate.NewRuntime(store, probe, counterv1.EventCodec{})
	mux := http.NewServeMux()
	mux.Handle(countersvcv1connect.NewCounterCommandServiceHandler(countersvcv1es.NewCounterCommandServiceHandler(countersvcv1es.CounterCommandServiceDeps{
		Runtime: rt, Authorize: authorizer{}, Identity: identity{subject: "alice"},
	})))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	client := countersvcv1connect.NewCounterCommandServiceClient(srv.Client(), srv.URL)
	if _, err := client.Init(ctx, connect.NewRequest(&countersvcv1.InitRequest{CounterId: "main", Command: &counterv1.Init{Max: 10}})); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Increment(ctx, connect.NewRequest(&countersvcv1.IncrementRequest{
		CounterId: "main", Command: &counterv1.Increment{By: 1, Actor: &counterv1.Principal{Subject: "mallory"}},
	})); err != nil {
		t.Fatal(err)
	}
	if seen != "alice" {
		t.Fatalf("decider saw principal %q, want alice", seen)
	}
}

func TestCommandServiceVersionConflict(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "alice")
	if _, err := h.client.Init(ctx, connect.NewRequest(&countersvcv1.InitRequest{CounterId: "main", Command: &counterv1.Init{Max: 100, Initial: 5}})); err != nil {
		t.Fatal(err)
	}
	if _, err := h.client.Increment(ctx, connect.NewRequest(&countersvcv1.IncrementRequest{CounterId: "main", Command: &counterv1.Increment{By: 10}})); err != nil {
		t.Fatal(err)
	}
	// Stale: the client thinks the stream is at v1, it is at v2.
	_, err := h.client.Increment(ctx, connect.NewRequest(&countersvcv1.IncrementRequest{
		CounterId: "main", Command: &counterv1.Increment{By: 1, ExpectedVersion: 1},
	}))
	var ce *connect.Error
	if !errors.As(err, &ce) || ce.Code() != connect.CodeFailedPrecondition {
		t.Fatalf("error = %v, want failed_precondition", err)
	}
	vc, ok := cmdsvc.VersionConflictDetail(err)
	if !ok {
		t.Fatalf("no VersionConflict detail on %v", err)
	}
	if vc.GetExpectedVersion() != 1 || vc.GetCurrentVersion() != 2 {
		t.Fatalf("detail = %+v, want expected 1 current 2", vc)
	}
	state := &counterv1.Counter{}
	if err := vc.GetCurrentState().UnmarshalTo(state); err != nil || state.GetCount() != 15 {
		t.Fatalf("current_state = %+v err=%v, want count 15", state, err)
	}
	// Nothing was appended by the stale command.
	sid, _ := es.NewStreamID("counter", "main")
	if _, v, _ := h.rt.Load(ctx, sid); v != 2 {
		t.Fatalf("version = %d, want 2", v)
	}
	// With the right version it goes through.
	if res, err := h.client.Increment(ctx, connect.NewRequest(&countersvcv1.IncrementRequest{
		CounterId: "main", Command: &counterv1.Increment{By: 1, ExpectedVersion: 2},
	})); err != nil || res.Msg.GetResult().GetVersion() != 3 {
		t.Fatalf("increment at v2: %v", err)
	}
}

func TestCommandServiceValidationAndErrors(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "alice")

	// protovalidate: counter_id longer than 16 chars (a valid slug otherwise).
	_, err := h.client.Increment(ctx, connect.NewRequest(&countersvcv1.IncrementRequest{
		CounterId: strings.Repeat("a", 20), Command: &counterv1.Increment{By: 1},
	}))
	var ce *connect.Error
	if !errors.As(err, &ce) || ce.Code() != connect.CodeInvalidArgument {
		t.Fatalf("validation error = %v, want invalid_argument", err)
	}
	if len(ce.Details()) == 0 {
		t.Fatal("validation error carries no violations detail")
	}
	// Required command missing.
	_, err = h.client.Increment(ctx, connect.NewRequest(&countersvcv1.IncrementRequest{CounterId: "main"}))
	if !errors.As(err, &ce) || ce.Code() != connect.CodeInvalidArgument {
		t.Fatalf("missing command error = %v, want invalid_argument", err)
	}
	// Invalid stream id (passes validation, fails es.NewStreamID).
	_, err = h.client.Init(ctx, connect.NewRequest(&countersvcv1.InitRequest{CounterId: "Not A Slug", Command: &counterv1.Init{}}))
	if !errors.As(err, &ce) || ce.Code() != connect.CodeInvalidArgument {
		t.Fatalf("stream id error = %v, want invalid_argument", err)
	}
	// Decider rejection through the app's ErrorMapper.
	if _, err := h.client.Init(ctx, connect.NewRequest(&countersvcv1.InitRequest{CounterId: "main", Command: &counterv1.Init{Max: 10}})); err != nil {
		t.Fatal(err)
	}
	_, err = h.client.Increment(ctx, connect.NewRequest(&countersvcv1.IncrementRequest{CounterId: "main", Command: &counterv1.Increment{By: 100}}))
	if !errors.As(err, &ce) || ce.Code() != connect.CodeOutOfRange {
		t.Fatalf("mapped domain error = %v, want out_of_range", err)
	}
	// Unmapped decider rejection: failed_precondition.
	_, err = h.client.Init(ctx, connect.NewRequest(&countersvcv1.InitRequest{CounterId: "main", Command: &counterv1.Init{Max: 10}}))
	if !errors.As(err, &ce) || ce.Code() != connect.CodeFailedPrecondition {
		t.Fatalf("second init error = %v, want failed_precondition", err)
	}
	// decided_at missing on Close: invalid_argument (ADR 0032 sentinel).
	_, err = h.client.Close(ctx, connect.NewRequest(&countersvcv1.CloseRequest{CounterId: "main", Command: &counterv1.Close{}}))
	if !errors.As(err, &ce) || ce.Code() != connect.CodeInvalidArgument {
		t.Fatalf("missing decided_at error = %v, want invalid_argument", err)
	}
	// decided_at outside the server-time skew: invalid_argument, nothing appended.
	backdated := timestamppb.New(time.Now().Add(-es.MaxDecisionSkew - time.Minute))
	_, err = h.client.Close(ctx, connect.NewRequest(&countersvcv1.CloseRequest{CounterId: "main", Command: &counterv1.Close{DecidedAt: backdated}}))
	if !errors.As(err, &ce) || ce.Code() != connect.CodeInvalidArgument {
		t.Fatalf("skewed decided_at error = %v, want invalid_argument", err)
	}
	sid, _ := es.NewStreamID("counter", "main")
	if _, v, _ := h.rt.Load(ctx, sid); v != 1 {
		t.Fatalf("version after skewed close = %d, want 1 (nothing appended)", v)
	}
	// Terminal stream: failed_precondition.
	if _, err := h.client.Close(ctx, connect.NewRequest(&countersvcv1.CloseRequest{CounterId: "main", Command: &counterv1.Close{DecidedAt: timestamppb.Now()}})); err != nil {
		t.Fatal(err)
	}
	_, err = h.client.Increment(ctx, connect.NewRequest(&countersvcv1.IncrementRequest{CounterId: "main", Command: &counterv1.Increment{By: 1}}))
	if !errors.As(err, &ce) || ce.Code() != connect.CodeFailedPrecondition || !strings.Contains(ce.Message(), "terminal") {
		t.Fatalf("terminal error = %v, want failed_precondition naming the stream", err)
	}
}

func TestSetWriteResultRejectsWrongShape(t *testing.T) {
	// A message without the three fields is an error, not a silent no-op.
	sid, _ := es.NewStreamID("counter", "main")
	if err := cmdsvc.SetWriteResult(&counterv1.Principal{}, sid, 1, 1); err == nil {
		t.Fatal("expected an error for a message without stream_id/version/global_position")
	}
	wr := &countersvcv1.WriteResult{}
	if err := cmdsvc.SetWriteResult(wr, sid, 3, 9); err != nil || wr.GetStreamId() != "counter:main" || wr.GetVersion() != 3 || wr.GetGlobalPosition() != 9 {
		t.Fatalf("write result = %+v err=%v", wr, err)
	}
}

// storeOf reaches the harness' store through the runtime's SQLite handle.
func storeOf(h harness) es.Store { return h.store() }

// ---- Idempotency-Key dedup (draft-ietf-httpapi-idempotency-key-header-07,
// design D6, finding F6) ----------------------------------------------------
//
// The dedup harness wires both pieces the draft flow needs: a
// sqlite-backed es.Deduplicator in Deps.Dedup and a runtime with
// deterministic event ids, so a retried or concurrent duplicate derives
// the event ids the first run stored and collides on the event-id
// constraint instead of double-appending. Conflict retry is wired the
// way a machine-facing edge wires it (ADR 0032): a concurrent duplicate
// that first loses the optimistic version race re-loads and then loses
// on the event-id constraint — the draft's 409 shape — instead of
// surfacing a bare version conflict. For the sequential tests the
// retry budget is inert (no conflict ever happens).

// newDedupHarness is newHarness plus the D6 wiring. wrap, when non-nil,
// wraps the sqlite dedup store — the concurrent test stages its race
// through one.
func newDedupHarness(t *testing.T, wrap func(inner es.Deduplicator) es.Deduplicator) harness {
	t.Helper()
	store, err := sqlite.Open(context.Background(), "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{}).
		WithDeterministicEventIDs().
		WithConflictRetry(2, func(int) time.Duration { return time.Millisecond })
	dedup := es.Deduplicator(sqlite.NewDedupStore(store))
	if wrap != nil {
		dedup = wrap(dedup)
	}
	handler := countersvcv1es.NewCounterCommandServiceHandler(countersvcv1es.CounterCommandServiceDeps{
		Runtime:   rt,
		Authorize: authorizer{},
		Identity:  identity{subject: "alice"},
		Dedup:     dedup,
		Errors: func(err error) *connect.Error {
			if errors.Is(err, counter.ErrOutOfRange) {
				return connect.NewError(connect.CodeOutOfRange, err)
			}
			return nil
		},
	})
	mux := http.NewServeMux()
	mux.Handle(countersvcv1connect.NewCounterCommandServiceHandler(handler))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return harness{
		client: countersvcv1connect.NewCounterCommandServiceClient(srv.Client(), srv.URL),
		rt:     rt,
		st:     store,
	}
}

// lookupBarrier makes the "both in-flight requests missed the lookup"
// precondition of the concurrent race deterministic instead of sleep
// roulette: each Lookup completes its inner lookup first, then waits
// until every participant has, so neither request reaches HandleAt
// before both lookups returned a miss. The wait honors ctx, so a broken
// flow surfaces as an error rather than a hang. One barrier, one round.
type lookupBarrier struct {
	inner es.Deduplicator
	n     int32
	seen  atomic.Int32
	done  chan struct{}
}

func newLookupBarrier(n int, inner es.Deduplicator) *lookupBarrier {
	return &lookupBarrier{inner: inner, n: int32(n), done: make(chan struct{})}
}

func (b *lookupBarrier) Lookup(ctx context.Context, key es.DedupKey) (es.DedupRecord, bool, error) {
	rec, found, err := b.inner.Lookup(ctx, key)
	if err != nil {
		return rec, found, err
	}
	if b.seen.Add(1) == b.n {
		close(b.done) // the last participant arrives; everyone may proceed
	}
	select {
	case <-b.done:
		return rec, found, err
	case <-ctx.Done():
		return es.DedupRecord{}, false, ctx.Err()
	}
}

func (b *lookupBarrier) Save(ctx context.Context, key es.DedupKey, rec es.DedupRecord) error {
	return b.inner.Save(ctx, key, rec)
}

// TestIdempotencyReplay (draft §2.6): the same key and the same payload
// replay the STORED response — identical bytes, no second append, the
// log still holds exactly one event. The replay provably is not a
// re-handle: a re-handled Init would be rejected "already initialized".
// A keyless request with the same payload is NOT replayed — the flow
// engages only for key-carrying requests — so it re-handles and gets
// exactly that rejection.
func TestIdempotencyReplay(t *testing.T) {
	ctx := context.Background()
	h := newDedupHarness(t, nil)

	send := func() *connect.Request[countersvcv1.InitRequest] {
		req := connect.NewRequest(&countersvcv1.InitRequest{
			CounterId: "main", Command: &counterv1.Init{Min: 0, Max: 100, Initial: 5},
		})
		req.Header().Set(cmdsvc.HdrIdempotencyKey, "replay-1")
		return req
	}
	first, err := h.client.Init(ctx, send())
	if err != nil {
		t.Fatalf("first init: %v", err)
	}
	if first.Msg.GetResult().GetVersion() != 1 || first.Msg.GetState().GetCount() != 5 {
		t.Fatalf("first result = %+v state = %+v, want v1 count 5", first.Msg.GetResult(), first.Msg.GetState())
	}

	// Same key, same payload: the stored response, verbatim.
	replay, err := h.client.Init(ctx, send())
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !proto.Equal(first.Msg, replay.Msg) {
		t.Fatalf("replay = %+v, want the stored response %+v", replay.Msg, first.Msg)
	}

	// The replay appended nothing: the log still holds exactly one event.
	sid, _ := es.NewStreamID("counter", "main")
	rows, err := storeOf(h).ReadStream(ctx, sid, 0, 0)
	if err != nil || len(rows) != 1 {
		t.Fatalf("stream rows after replay = %d (err %v), want 1 — a replay must not append", len(rows), err)
	}

	// The same payload WITHOUT the key is not replayed: it re-handles and
	// the decider rejects the second Init — proof the flow engages only
	// for key-carrying requests.
	_, err = h.client.Init(ctx, connect.NewRequest(&countersvcv1.InitRequest{
		CounterId: "main", Command: &counterv1.Init{Min: 0, Max: 100, Initial: 5},
	}))
	var ce *connect.Error
	if !errors.As(err, &ce) || ce.Code() != connect.CodeFailedPrecondition {
		t.Fatalf("keyless request = %v, want failed_precondition (re-handled, not replayed)", err)
	}
}

// TestIdempotencyFingerprintMismatch (draft §2.7): the same key with a
// different payload is key reuse — aborted, the message naming HTTP 422
// semantics — and the original record survives it intact: replaying the
// FIRST payload still returns the original stored response, and the log
// still holds exactly one event.
func TestIdempotencyFingerprintMismatch(t *testing.T) {
	ctx := context.Background()
	h := newDedupHarness(t, nil)

	orig := connect.NewRequest(&countersvcv1.InitRequest{
		CounterId: "main", Command: &counterv1.Init{Min: 0, Max: 100, Initial: 5},
	})
	orig.Header().Set(cmdsvc.HdrIdempotencyKey, "reuse-1")
	first, err := h.client.Init(ctx, orig)
	if err != nil {
		t.Fatalf("first init: %v", err)
	}

	// Same key, different payload: key reuse, HTTP 422 semantics.
	reuse := connect.NewRequest(&countersvcv1.InitRequest{
		CounterId: "main", Command: &counterv1.Init{Min: 0, Max: 50, Initial: 5},
	})
	reuse.Header().Set(cmdsvc.HdrIdempotencyKey, "reuse-1")
	_, err = h.client.Init(ctx, reuse)
	var ce *connect.Error
	if !errors.As(err, &ce) || ce.Code() != connect.CodeAborted {
		t.Fatalf("key reuse error = %v, want aborted", err)
	}
	if !strings.Contains(ce.Message(), "422") {
		t.Fatalf("key reuse message = %q, want it to name HTTP 422 semantics", ce.Message())
	}

	// The record is intact: the first payload still replays the original
	// response, and the rejected request appended nothing.
	again := connect.NewRequest(&countersvcv1.InitRequest{
		CounterId: "main", Command: &counterv1.Init{Min: 0, Max: 100, Initial: 5},
	})
	again.Header().Set(cmdsvc.HdrIdempotencyKey, "reuse-1")
	replay, err := h.client.Init(ctx, again)
	if err != nil {
		t.Fatalf("replay after reuse: %v", err)
	}
	if !proto.Equal(first.Msg, replay.Msg) {
		t.Fatalf("replay after reuse = %+v, want the original response %+v", replay.Msg, first.Msg)
	}
	sid, _ := es.NewStreamID("counter", "main")
	rows, err := storeOf(h).ReadStream(ctx, sid, 0, 0)
	if err != nil || len(rows) != 1 {
		t.Fatalf("stream rows = %d (err %v), want 1 — the reused key must not append", len(rows), err)
	}
}

// TestIdempotencyConcurrentConflict (draft §2.6's concurrent window):
// two requests with the same key in flight at once both miss the dedup
// lookup — the barrier in the wrapped Dedup holds each lookup until both
// completed, deterministic staging, not sleep roulette — so both reach
// HandleAt with the same derived CommandID and deterministic event ids:
// one insert wins, the loser collides on the event-id constraint and is
// reported aborted, the message naming HTTP 409 semantics. The loser
// appended nothing: the stream holds Init plus exactly one Incremented.
func TestIdempotencyConcurrentConflict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h := newDedupHarness(t, func(inner es.Deduplicator) es.Deduplicator {
		return newLookupBarrier(2, inner)
	})

	if _, err := h.client.Init(ctx, connect.NewRequest(&countersvcv1.InitRequest{
		CounterId: "main", Command: &counterv1.Init{Min: 0, Max: 100, Initial: 5},
	})); err != nil {
		t.Fatal(err)
	}

	const goroutines = 2
	var wg sync.WaitGroup
	results := make([]*connect.Response[countersvcv1.IncrementResponse], goroutines)
	errs := make([]error, goroutines)
	for i := range goroutines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := connect.NewRequest(&countersvcv1.IncrementRequest{
				CounterId: "main", Command: &counterv1.Increment{By: 1},
			})
			req.Header().Set(cmdsvc.HdrIdempotencyKey, "concurrent-1")
			results[i], errs[i] = h.client.Increment(ctx, req)
		}(i)
	}
	wg.Wait()

	var wins int
	for i, err := range errs {
		if err == nil {
			wins++
			if results[i].Msg.GetResult().GetVersion() != 2 || results[i].Msg.GetState().GetCount() != 6 {
				t.Fatalf("winner result = %+v state = %+v, want v2 count 6", results[i].Msg.GetResult(), results[i].Msg.GetState())
			}
			continue
		}
		var ce *connect.Error
		if !errors.As(err, &ce) || ce.Code() != connect.CodeAborted {
			t.Fatalf("goroutine %d error = %v, want aborted (HTTP 409 semantics)", i, err)
		}
		if !strings.Contains(ce.Message(), "409") {
			t.Fatalf("loser message = %q, want it to name HTTP 409 semantics", ce.Message())
		}
	}
	if wins != 1 {
		t.Fatalf("wins = %d, want exactly one — the loser must be aborted", wins)
	}

	// The loser appended nothing: the stream is at v2 with exactly two
	// events (Initialized + the winner's Incremented).
	sid, _ := es.NewStreamID("counter", "main")
	rows, err := storeOf(h).ReadStream(ctx, sid, 0, 0)
	if err != nil || len(rows) != 2 {
		t.Fatalf("stream rows after the race = %d (err %v), want 2 — the loser must not append", len(rows), err)
	}
	if _, v, _ := h.rt.Load(ctx, sid); v != 2 {
		t.Fatalf("version after the race = %d, want 2", v)
	}
}
