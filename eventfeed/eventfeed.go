// Package eventfeed binds the fenced feed protocol to the projection
// runner: it adapts a feed.Server into a projection.Feed (ADR 0030 §2) and
// enforces in code the at-least-once invariant that was previously only a
// doc comment on the runner — the checkpoint is acked only after handle
// returns nil, and never past what was delivered and handled (review R8).
//
// The adapter is deliberately thin. The server owns the lease, the
// delivery cursor and the reconnect resume point; this package owns only
// the ack policy — when the consumer's receipt is sent — and the
// projection.Feed shape the runner consumes. Acknowledgements are
// coalesced: one fenced ack per full page, plus one on the idle tick when
// events trickled in sub-page, plus one at stream end — never one per
// event — so a fleet of subscribers cannot stampede the checkpoint table,
// and a crash redelivers only what was handled since the last successful
// ack: one page when acks are healthy (the marker dedups).
package eventfeed

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/feed"
	"github.com/laenenai/es-lite/projection"
)

// Defaults applied by New to zero Config fields.
const (
	// defaultBatchSize is the ack window when Config.BatchSize is unset:
	// the feed server's own default page size. The server's configured
	// batch is not observable through the feed API, so a server built
	// with a non-default Config.Batch should set BatchSize to match.
	defaultBatchSize = 500
	// defaultAckInterval is the idle ack cadence when Config.AckInterval
	// is unset.
	defaultAckInterval = 250 * time.Millisecond
	// flushAckTimeout bounds the stream-end ack, which runs after the
	// caller's ctx has ended, on a detached context — the same shape as
	// the server's bounded lease release.
	flushAckTimeout = 5 * time.Second
)

// Config is the consumer adapter's dependencies and tunables. Server,
// Definition and Scope are required; zero values elsewhere take the
// defaults documented on the fields.
type Config struct {
	// Server is the feed core this consumer subscribes to.
	Server *feed.Server
	// Definition is the projection's metadata: its Name keys the
	// checkpoint and the lease (Request.Projection is Definition.Name),
	// its Consumes is the authorized type set the server filters pages
	// by, and its Hash() is the fingerprint every ack carries — the
	// identity the checkpoint must keep matching.
	Definition projection.Definition
	// Scope, Instance and Consumer identify the subscription: the
	// checkpoint/lease key, and the consumer name Status reports.
	Scope, Instance, Consumer string
	// BatchSize is the ack window: one fenced ack per BatchSize handled
	// events, and the page size the server is asked for, so a full page
	// and a full window coincide by construction and the crash-redelivery
	// bound is one page. Default: the feed server's default page size
	// (500).
	BatchSize int
	// AckInterval is how long a trailing sub-page may wait for its ack
	// while the stream is live, and the retry cadence when handle fails.
	// Default 250ms.
	AckInterval time.Duration
	// Observer is the optional observability sink. Poison — the oversized
	// event the log can never deliver — surfaces through OnPoison on
	// every reconnect attempt until an operator skips past the position;
	// nil behaves exactly like feed.NoopObserver.
	Observer feed.Observer
}

// Feed is one subscription over a feed.Server: a projection.Feed that
// reconnects until its context ends. New constructs it.
type Feed struct {
	cfg   Config
	token string
	batch int
	tick  time.Duration
	inner *projection.ReconnectingFeed
}

var _ projection.Feed = (*Feed)(nil)

// New validates cfg and returns a Feed that streams cfg.Definition's
// events from cfg.Server. It panics on the static misconfigurations the
// server would otherwise reject on every attempt — a nil Server, an empty
// Scope, a Definition without a name or consumed types — because they are
// programming errors, not runtime conditions: retrying them would only
// hide the mistake behind a reconnect loop.
func New(cfg Config) *Feed {
	switch {
	case cfg.Server == nil:
		panic("eventfeed: Config.Server is required")
	case cfg.Scope == "":
		panic("eventfeed: Config.Scope is required")
	case cfg.Definition.Name == "":
		panic("eventfeed: Config.Definition.Name is required")
	case len(cfg.Definition.Consumes) == 0:
		panic("eventfeed: Config.Definition.Consumes is required")
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.AckInterval <= 0 {
		cfg.AckInterval = defaultAckInterval
	}
	if cfg.Observer == nil {
		cfg.Observer = feed.NoopObserver{}
	}
	f := &Feed{
		cfg:   cfg,
		token: newToken(),
		batch: cfg.BatchSize,
		tick:  cfg.AckInterval,
	}
	// Each reconnect opens a fresh attempt; the token — the lease identity
	// — is the Feed's, so an attempt re-acquires its own subscription.
	f.inner = &projection.ReconnectingFeed{
		Open: func(context.Context) (projection.Feed, error) {
			return &attempt{feed: f}, nil
		},
		// Min and Max stay zero: ReconnectingFeed's own defaults are the
		// 250ms initial backoff and 30s ceiling this adapter is specified
		// with.
	}
	return f
}

// Feed implements projection.Feed. It blocks until ctx ends —
// reconnecting through the server whenever the stream drops — and
// returns ctx.Err() then. A Feed is one subscription, so call Feed once
// at a time; concurrent calls would race for one lease.
func (f *Feed) Feed(ctx context.Context, handle func(ctx context.Context, e es.Envelope) error) error {
	return f.inner.Feed(ctx, handle)
}

// newToken mints the stable lease-owner token: 8 random bytes, the same
// shape as the server's per-process owner prefix, unique across processes
// so two feed processes can never pass each other's fence.
//
// The token is stable for the Feed's whole life (the spec leaves the
// reconnect token policy to the builder; this is the stable-token
// choice): a reconnect re-acquires under the same owner, so the adapter
// self-steals its own lingering lease instead of waiting out the TTL —
// a same-owner acquire succeeds where a fresh token would loop on
// LeaseHeld. The trade-off (spec §5.4): because Renew carries no epoch,
// a zombie stream of this same Feed can keep renewing for a bounded
// overlap window after a self-steal, so both the old and the new stream
// deliver — duplicates, never loss: the marker dedups, and the
// monotonic cursor never advances past a position a holder actually
// delivered. A foreign owner still fails closed: cross-owner takeover
// is rejected at acquire, and the loser's next renewal fails.
func newToken() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Entropy failure is an OS-level fault, and a constant fallback
		// would collide across processes and defeat the lease fence, so
		// fail fast instead of manufacturing an owner.
		panic(fmt.Sprintf("eventfeed: generate owner token: %v", err))
	}
	return hex.EncodeToString(b[:])
}

// attempt is one subscription attempt: one Subscribe call, one lease, one
// pending-ack window. ReconnectingFeed opens a fresh attempt after every
// drop. An attempt that ends before the caller's ctx does — poison, a
// lost lease, a send error — leaves its unacked window to the next
// attempt: the lease is already released, so no ack can pass the fence
// anymore, and the reconnect redelivers from the last acknowledged
// position with the marker deduplicating. Events delivered while a
// stream-end ack is still in flight are unacked too, and redeliver the
// same way — the bound is everything handled since the last successful
// ack, one page when acks are healthy.
type attempt struct {
	feed   *Feed
	handle func(ctx context.Context, e es.Envelope) error

	// mu guards the ack window: pending is the highest handled position,
	// count the events handled since the last ack. count reaching the
	// batch size is the page-boundary trigger; the tick and the
	// stream-end flush are the others.
	mu      sync.Mutex
	pending uint64
	count   int
}

// request is the subscription this adapter presents to the server: the
// caller-supplied, stable Owner token (so the adapter's acks are fenced
// to its own lease), the definition's authorized type set, and the ack
// window as the page size.
func (a *attempt) request() feed.Request {
	return feed.Request{
		Scope:      a.feed.cfg.Scope,
		Projection: a.feed.cfg.Definition.Name,
		Instance:   a.feed.cfg.Instance,
		Consumer:   a.feed.cfg.Consumer,
		Owner:      a.feed.token,
		Definition: a.feed.cfg.Definition,
		BatchSize:  a.feed.batch,
	}
}

// Feed runs the attempt: it subscribes and streams to handle until the
// stream drops or the caller's ctx ends.
//
// runCtx is deliberately not derived from ctx. The caller's cancellation
// must be translated, not inherited: when ctx ends, the stream-end ack
// has to fire while the subscription — and its lease — is still alive,
// and only then may the stream be cancelled. A child context would tear
// the stream down and release the lease before the ack could pass the
// fence, leaving the trailing window to redelivery on every shutdown.
func (a *attempt) Feed(ctx context.Context, handle func(ctx context.Context, e es.Envelope) error) error {
	a.handle = handle
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()

	ticker := time.NewTicker(a.feed.tick)
	defer ticker.Stop()
	loopDone := make(chan struct{})
	go a.loop(ctx, runCtx, ticker.C, cancel, loopDone)

	err := a.feed.cfg.Server.Subscribe(runCtx, a.request(), a)
	cancel()
	<-loopDone

	// Poison — the oversized event the log can never deliver — surfaces
	// through the observer rather than being swallowed into the
	// reconnect loop's backoff. The stream is dead and every reconnect
	// re-hits the same row, so the loop retries with backoff and keeps
	// reporting until an operator skips past the position (feed.Skip).
	var poison *feed.EventTooLargeError
	if errors.As(err, &poison) {
		a.feed.cfg.Observer.OnPoison(poison)
	}
	if cerr := ctx.Err(); cerr != nil {
		return cerr
	}
	return err
}

// loop is the attempt's ack clock: it flushes the pending window on the
// tick while the stream is live, and exactly once more at the caller's
// ctx end — the stream-end ack — before the stream is torn down. A tick
// ack the fence rejects means the lease is gone; the loop ends the
// attempt so the reconnect re-acquires instead of acking into a fence
// that can never pass again.
func (a *attempt) loop(ctx, runCtx context.Context, tick <-chan time.Time, cancel context.CancelFunc, done chan<- struct{}) {
	defer close(done)
	for {
		select {
		case <-ctx.Done():
			// The stream-end ack: ctx is already dead, so it runs on a
			// detached, bounded context like the server's own lease
			// release. A rejection here (the lease was lost under us) is
			// dropped: the reconnect redelivers the window and the
			// marker dedups — never an advanced cursor.
			flushCtx, flushCancel := context.WithTimeout(context.WithoutCancel(ctx), flushAckTimeout)
			_ = a.ack(flushCtx)
			flushCancel()
			cancel()
			return
		case <-tick:
			if err := a.ack(runCtx); err != nil {
				cancel()
				return
			}
		case <-runCtx.Done():
			return
		}
	}
}

// Send implements feed.Sink: it delivers one envelope to handle and, only
// once handle returns nil, accounts the position in the ack window — the
// at-least-once invariant. The full-window ack happens here, inside the
// stream: the lease is held, the fence passes, and the checkpoint never
// advances past a position that was delivered and handled.
//
// A handle error means "not applied — redeliver later" (the
// projection.Feed contract): the same envelope is retried after one ack
// interval, mirroring LogFeed's poll retry, so a failing handler does not
// churn the lease.
func (a *attempt) Send(ctx context.Context, env es.Envelope) error {
	for {
		if err := a.handle(ctx, env); err == nil {
			break
		}
		if err := sleep(ctx, a.feed.tick); err != nil {
			return err
		}
	}
	a.mu.Lock()
	a.pending = env.GlobalPosition
	a.count++
	full := a.count >= a.feed.batch
	a.mu.Unlock()
	if full {
		if err := a.ack(ctx); err != nil {
			return fmt.Errorf("eventfeed: ack at %d: %w", env.GlobalPosition, err)
		}
	}
	return nil
}

// ack flushes the ack window: it claims the window atomically, then sends
// one fenced ack — the full Ack shape, with the stable owner token and
// the definition fingerprint. The claim is the deduplication: a full
// page, a tick and the stream-end flush that race always ack disjoint
// windows, so a window is acked exactly once. A rejected ack leaves the
// window un-acked: the positions are redelivered after the reconnect and
// deduplicated by the marker; the cursor never advances past what was
// delivered.
func (a *attempt) ack(ctx context.Context) error {
	a.mu.Lock()
	if a.count == 0 {
		a.mu.Unlock()
		return nil
	}
	pos := a.pending
	a.count = 0
	a.mu.Unlock()
	return a.feed.cfg.Server.Checkpoint(ctx, feed.Ack{
		Scope:      a.feed.cfg.Scope,
		Projection: a.feed.cfg.Definition.Name,
		Instance:   a.feed.cfg.Instance,
		Owner:      a.feed.token,
		Position:   pos,
		Definition: a.feed.cfg.Definition.Hash(),
	})
}

// sleep waits for d, or returns ctx's error when ctx ends first.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
