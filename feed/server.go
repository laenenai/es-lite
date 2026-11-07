package feed

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// Defaults applied by NewServer to zero Config fields.
const (
	defaultBatch           = 500
	defaultMaxBatch        = 1000
	defaultMaxPayloadBytes = 8 << 20  // 8 MiB
	defaultMaxBatchBytes   = 16 << 20 // 16 MiB
	defaultPoll            = 250 * time.Millisecond
	defaultLeaseTTL        = 30 * time.Second
	defaultRenewEvery      = 10 * time.Second
)

// leaseReleaseTimeout bounds the context used to release a lease after the
// stream's own context has ended.
const leaseReleaseTimeout = 5 * time.Second

// Config is the Server's dependencies and tunables. Checkpoints, Leases and
// Logs are required; zero values elsewhere take the defaults documented on the
// fields.
type Config struct {
	// Checkpoints persists delivery cursors.
	Checkpoints CheckpointStore
	// Leases fences one active subscriber per subscription key.
	Leases LeaseStore
	// Logs opens the per-scope event log.
	Logs Logs

	// Waiter is an optional wakeup source; nil polls every Poll.
	Waiter Waiter
	// Clock supplies wall time for lease owner identity and dead-letter
	// timestamps; nil is time.Now.
	Clock Clock
	// DeadLetters is Skip's optional audit sink. When non-nil, Skip
	// records one dead letter per call before advancing the checkpoint;
	// nil means Skip still advances but records nothing — the skip works,
	// only the audit trail is absent.
	DeadLetters DeadLetterStore
	// Observer is the optional observability sink (OnPage, OnCheckpoint,
	// OnLeaseLost, OnPoison, OnStall). nil behaves exactly like
	// NoopObserver: no signals, no cost.
	Observer Observer

	// Batch is the page size when Request.BatchSize is zero. Default 500.
	Batch int
	// MaxBatch caps Request.BatchSize. Default 1000; Batch is clamped to it.
	MaxBatch int
	// MaxPayloadBytes rejects a single stored payload larger than this before
	// it is sent. Default 8 MiB; zero takes the default and a negative value
	// disables the check.
	MaxPayloadBytes int
	// MaxBatchBytes is the byte budget passed to Log.ReadAllBounded, which
	// requires a positive budget. Default 16 MiB; zero and negative values
	// take the default, so the log never receives a non-positive budget.
	MaxBatchBytes int
	// Poll is the idle wait between short pages, scaled by a ±20% jitter
	// (per idle wait) so a fleet of streams started together does not poll
	// in lockstep. Default 250ms.
	Poll time.Duration
	// LeaseTTL is the lease lifetime. Default 30s.
	LeaseTTL time.Duration
	// RenewEvery is the renew interval. Default 10s, and always kept below
	// LeaseTTL.
	RenewEvery time.Duration
}

// Server is the transport-agnostic feed core.
type Server struct {
	cfg    Config
	prefix string
	seq    atomic.Uint64

	// jitter scales one idle poll wait (spec §5.12): NewServer wires
	// defaultJitter — the ±20% that keeps a fleet of streams started
	// together from polling in lockstep — and internal tests pin fixed
	// factors. Nil means no jitter.
	jitter func(time.Duration) time.Duration
}

// NewServer validates cfg, fills defaults and returns a Server. It does not
// open any log or acquire any lease.
func NewServer(cfg Config) (*Server, error) {
	switch {
	case cfg.Checkpoints == nil:
		return nil, errors.New("feed: Config.Checkpoints is required")
	case cfg.Leases == nil:
		return nil, errors.New("feed: Config.Leases is required")
	case cfg.Logs == nil:
		return nil, errors.New("feed: Config.Logs is required")
	}
	if cfg.Clock == nil {
		cfg.Clock = systemClock{}
	}
	if cfg.Observer == nil {
		cfg.Observer = NoopObserver{}
	}
	if cfg.Batch <= 0 {
		cfg.Batch = defaultBatch
	}
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = defaultMaxBatch
	}
	if cfg.Batch > cfg.MaxBatch {
		cfg.Batch = cfg.MaxBatch
	}
	if cfg.MaxPayloadBytes == 0 {
		cfg.MaxPayloadBytes = defaultMaxPayloadBytes
	}
	if cfg.MaxBatchBytes <= 0 {
		cfg.MaxBatchBytes = defaultMaxBatchBytes
	}
	if cfg.Poll <= 0 {
		cfg.Poll = defaultPoll
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = defaultLeaseTTL
	}
	if cfg.RenewEvery <= 0 {
		cfg.RenewEvery = defaultRenewEvery
	}
	// Renewing at or after expiry would let the lease lapse mid-stream; derive
	// a safe interval instead of failing a misconfiguration at runtime.
	if cfg.RenewEvery >= cfg.LeaseTTL {
		cfg.RenewEvery = cfg.LeaseTTL / 3
		if cfg.RenewEvery <= 0 {
			cfg.RenewEvery = cfg.LeaseTTL / 2
		}
		if cfg.RenewEvery <= 0 {
			return nil, fmt.Errorf("feed: Config.LeaseTTL %v is too small to renew safely", cfg.LeaseTTL)
		}
	}
	prefix, err := newPrefix()
	if err != nil {
		return nil, err
	}
	return &Server{cfg: cfg, prefix: prefix, jitter: defaultJitter}, nil
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// randRead supplies newPrefix's entropy. It is a variable so the internal
// test can force an entropy failure.
var randRead = rand.Read

// defaultJitter is the production idle-wait jitter source: it scales a poll
// by a uniform random factor in [0.8, 1.2) — the ±20% of spec §5.12 — drawn
// per idle wait, so a fleet of streams started together random-walks out of
// lockstep instead of polling as one herd that micro-bursts the connection
// pool. math/rand/v2's top-level functions are safe for concurrent use.
var defaultJitter = func(d time.Duration) time.Duration {
	return time.Duration(float64(d) * (0.8 + 0.4*mathrand.Float64()))
}

// newPrefix returns a per-Server random identifier so lease owners are unique
// across processes without another dependency. Without entropy no safe owner
// can be minted — a constant prefix would collide across processes — so the
// error is propagated instead of masked.
func newPrefix() (string, error) {
	var b [8]byte
	if _, err := randRead(b[:]); err != nil {
		return "", fmt.Errorf("feed: generate lease owner prefix: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// newOwner returns a process-unique lease owner. The sequence keeps owners
// distinct within one process; the random prefix keeps them distinct between
// processes.
func (s *Server) newOwner() string {
	return fmt.Sprintf("%s-%d-%d", s.prefix, s.cfg.Clock.Now().UnixNano(), s.seq.Add(1))
}

// Subscribe streams req's events to sink until ctx ends or an error stops the
// stream. It acquires the subscription lease first, resumes from the stored
// checkpoint (clamped to the log head), then reads, filters and sends pages.
// The lease is renewed on its own goroutine; a failed renewal cancels the
// stream with a *LeaseLostError. Subscribe always releases the lease before
// returning, even when the stream failed.
//
// A checkpoint last acknowledged under a different definition fails the
// stream immediately with an error matching ErrDefinitionChanged: resuming
// would silently skip the history of any newly consumed type below the
// stored position. ResetCheckpoint under the new definition is the
// recovery; it re-identifies the checkpoint so the next Subscribe serves.
//
// The lease owner is req.Owner verbatim, so the consumer can fence its
// acknowledgements (Ack.Owner must match the holder). An empty req.Owner
// makes the server mint an opaque owner, and such a stream is not ackable.
//
// Subscribe never saves checkpoints. The consumer acknowledges with
// Checkpoint, so reconnecting resumes at its last acknowledged batch.
func (s *Server) Subscribe(ctx context.Context, req Request, sink Sink) (err error) {
	if err := validateRequest(req, sink); err != nil {
		return err
	}
	owner := req.Owner
	if owner == "" {
		owner = s.newOwner()
	}

	acquired, err := s.cfg.Leases.Acquire(ctx, req.Scope, req.Projection, req.Instance, owner, req.Consumer, s.cfg.LeaseTTL)
	if err != nil {
		aerr := fmt.Errorf("feed: acquire lease %s/%s/%s: %w", req.Scope, req.Projection, req.Instance, err)
		// A store may report an error after it took the lease (a failed
		// post-commit step, for example). (true, err) means we own it, so
		// release it rather than leaking the key.
		if acquired {
			if rerr := s.releaseLease(ctx, req.Scope, req.Projection, req.Instance, owner); rerr != nil {
				aerr = errors.Join(aerr, fmt.Errorf("feed: release lease %s/%s/%s after acquire error: %w", req.Scope, req.Projection, req.Instance, rerr))
			}
		}
		return aerr
	}
	if !acquired {
		lease, held, herr := s.cfg.Leases.Holder(ctx, req.Scope, req.Projection, req.Instance)
		if herr != nil {
			return fmt.Errorf("feed: acquire lease %s/%s/%s: %w", req.Scope, req.Projection, req.Instance, herr)
		}
		return &LeaseHeldError{Key: fmt.Sprintf("%s/%s/%s", req.Scope, req.Projection, req.Instance), Lease: lease, Held: held}
	}

	// runCtx is cancelled by the caller's ctx or by a failed renewal, so the
	// read loop and its sink both stop promptly.
	runCtx, cancel := context.WithCancel(ctx)
	lost := make(chan error, 1)
	var renewals sync.WaitGroup
	renewals.Add(1)
	go func() {
		defer renewals.Done()
		s.renew(runCtx, req.Scope, req.Projection, req.Instance, owner, lost, cancel)
	}()

	// Release ordering: stop the stream, wait for the renewer to exit, then
	// release. A Renew can therefore never race the Release and resurrect the
	// lease. The release uses a context detached from the caller's, because
	// cancellation must not leak the lease.
	defer func() {
		cancel()
		renewals.Wait()
		if rerr := s.releaseLease(ctx, req.Scope, req.Projection, req.Instance, owner); rerr != nil {
			err = errors.Join(err, fmt.Errorf("feed: release lease %s/%s/%s: %w", req.Scope, req.Projection, req.Instance, rerr))
		}
	}()

	streamErr := s.stream(runCtx, req, sink, s.batchSize(req.BatchSize))

	// Collect the independent causes instead of letting one silently replace
	// another. A stream error caused by our own cancellation, or by the
	// caller's own deadline arriving, is derived, not a diagnosis, and is
	// dropped; a real send/read error that races a lease loss is joined so
	// both remain visible.
	var leaseErr error
	select {
	case leaseErr = <-lost:
	default:
	}
	var causes []error
	if leaseErr != nil {
		causes = append(causes, leaseErr)
	}
	callerExpired := false
	if callerErr := ctx.Err(); callerErr != nil {
		callerExpired = true
		causes = append(causes, callerErr)
	}
	if len(causes) > 0 {
		// The stream runs under a child of ctx, so a caller deadline reaches
		// it too: the stream's own deadline error is the caller's error echoed
		// back, and joining it would report the same deadline twice.
		derived := errors.Is(streamErr, context.Canceled) ||
			(callerExpired && errors.Is(streamErr, context.DeadlineExceeded))
		if streamErr != nil && !derived {
			causes = append(causes, streamErr)
		}
		if len(causes) == 1 {
			return causes[0]
		}
		return errors.Join(causes...)
	}
	return streamErr
}

// releaseLease releases the subscription key for owner on a context detached
// from the stream's cancellation, bounded by leaseReleaseTimeout, so a
// cancelled stream still frees its key and values (tenant scope, tracing)
// survive.
func (s *Server) releaseLease(ctx context.Context, scope, projection, instance, owner string) error {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), leaseReleaseTimeout)
	defer cancel()
	return s.cfg.Leases.Release(releaseCtx, scope, projection, instance, owner)
}

// renew extends the lease every RenewEvery until ctx ends. On the first failed
// renewal it records a *LeaseLostError and cancels the stream. The error is
// buffered and recorded before the cancel, so Subscribe can always observe it.
func (s *Server) renew(ctx context.Context, scope, projection, instance, owner string, lost chan<- error, cancel context.CancelFunc) {
	ticker := time.NewTicker(s.cfg.RenewEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		ok, rerr := s.cfg.Leases.Renew(ctx, scope, projection, instance, owner, s.cfg.LeaseTTL)
		if ctx.Err() != nil {
			return // cancellation (caller or our own) ended the stream
		}
		if ok && rerr == nil {
			continue
		}
		if rerr == nil {
			rerr = errLeaseExpired
		}
		s.cfg.Observer.OnLeaseLost(scope, projection, instance, owner)
		select {
		case lost <- &LeaseLostError{Key: fmt.Sprintf("%s/%s/%s", scope, projection, instance), Owner: owner, Err: rerr}:
		default:
		}
		cancel()
		return
	}
}

// stallProbeEvery bounds the OnStall probe cadence: a stream idling on
// empty pages or waiting out a blocked clamp consults
// Log.MatchingRawHead at most once per window, so a fleet of idle or
// clamped subscribers cannot stampede the log with probes (spec
// §5.12). Both probes — the read loop's and the clamp loop's — share
// one window per stream.
const stallProbeEvery = 3 * time.Second

// clampProbeThreshold is N, the consecutive-observation count the clamp
// loop's probe requires before reporting: OnStall fires after N=2
// consecutive rate-limited probes — spaced by stallProbeEvery — each
// observed head < pos with matchingRaw >= pos. The cadence starts
// unarmed, so the first probe runs on the clamp's first re-head and the
// second after one full window: a block persistent for about one probe
// window signals, while an ordinary in-flight transaction that settles
// inside the window stays silent.
const clampProbeThreshold = 2

// stream is the read/send loop. It runs under runCtx, which the renewer may
// cancel; Subscribe translates the result. The event type set is the
// request's authorized Definition.Consumes, never a caller-built list.
func (s *Server) stream(ctx context.Context, req Request, sink Sink, batch int) error {
	name := req.Definition.Name
	log, err := s.cfg.Logs.Open(ctx, req.Scope)
	if err != nil {
		return fmt.Errorf("feed: open log for scope %q: %w", req.Scope, err)
	}
	cp, found, err := s.cfg.Checkpoints.Checkpoint(ctx, req.Scope, req.Projection, req.Instance)
	if err != nil {
		return fmt.Errorf("feed: load checkpoint %s/%s/%s: %w", req.Scope, name, req.Instance, err)
	}
	var pos uint64
	if found {
		pos = cp.Position
	}
	// Fail closed on definition drift (spec §5.5): the checkpoint names
	// the definition its position was acknowledged under, and resuming
	// under a different one would silently skip the history of any newly
	// consumed type below the stored position. A missing fingerprint —
	// a checkpoint from before the fingerprint existed, or an ack that
	// carried none — does not fail; only a recorded mismatch does.
	// Recovery is explicit: ResetCheckpoint under the new definition.
	requestHash := req.Definition.Hash()
	if cp.DefinitionHash != "" && cp.DefinitionHash != requestHash {
		return &DefinitionChangedError{
			Projection:  name,
			StoredHash:  cp.DefinitionHash,
			RequestHash: requestHash,
		}
	}
	head, err := log.Head(ctx)
	if err != nil {
		return fmt.Errorf("feed: head for scope %q: %w", req.Scope, err)
	}
	// Stall detection state (spec §5.8): the probe is rate-limited and
	// one OnStall fires per stall episode, re-armed by the next page
	// that delivers. Both probes share this state: the read loop's
	// empty-page probe and the clamp loop's blocked-clamp probe space
	// their MatchingRawHead consultations on one cadence, and an
	// episode — from the first report to the next delivered page —
	// reports exactly once, whichever loop observed it.
	var (
		lastProbe     time.Time
		stallReported bool
	)
	probeStall := func() {
		if stallReported || time.Since(lastProbe) < stallProbeEvery {
			return
		}
		lastProbe = time.Now()
		// The probe compares against the subscription's own backlog —
		// the raw head over its consumed types — never the all-type
		// raw head: a filtered stream caught up on its types is not
		// stalled, however many unrelated events land above it.
		matchingRaw, rerr := log.MatchingRawHead(ctx, req.Definition.Consumes)
		if rerr != nil {
			return // observability must never kill the stream
		}
		if matchingRaw > pos {
			stallReported = true
			s.cfg.Observer.OnStall(pos, matchingRaw)
		}
	}

	// probeClamp observes the clamp loop's blocked wait (review F15):
	// the resume checkpoint sits above the settled head while the
	// committed matching backlog still covers it — the position is
	// alive, only the fence is wedged. Rate-limited on the shared
	// cadence, it counts consecutive observations of that regime and
	// reports OnStall(pos, matchingRaw) after clampProbeThreshold of
	// them, latching the same stallReported the read loop's probe
	// re-arms on delivery — one report per episode, not one per re-head.
	// An observation of anything else — the fence cleared, or the
	// matching backlog no longer covers the position — breaks the streak.
	clampStreak := 0
	probeClamp := func() {
		if stallReported || time.Since(lastProbe) < stallProbeEvery {
			return
		}
		lastProbe = time.Now()
		matchingRaw, rerr := log.MatchingRawHead(ctx, req.Definition.Consumes)
		if rerr != nil {
			return // observability must never kill the stream
		}
		if head >= pos || matchingRaw < pos {
			clampStreak = 0
			return
		}
		clampStreak++
		if clampStreak >= clampProbeThreshold {
			stallReported = true
			s.cfg.Observer.OnStall(pos, matchingRaw)
		}
	}

	// A checkpoint ahead of the settled head is ambiguous (spec §5.1):
	// either the log was restored to an earlier point or is empty —
	// clamp locally, never rewriting the consumer's checkpoint — or the
	// head is only transiently low because a fence-bounded Log (Postgres)
	// reports 0 while a transaction blocks the fence, and clamping would
	// replay the whole log below the checkpoint. RawHead separates the
	// regimes: raw >= pos proves the checkpoint's position is still in
	// the committed log, so the fence is merely blocked and the stream
	// waits and re-heads; only raw < pos (the position is gone) clamps.
	// The wait is not silent: a block that persists across re-heads is
	// reported through the shared stall signal.
	for pos > head {
		raw, rerr := log.RawHead(ctx)
		if rerr != nil {
			return fmt.Errorf("feed: raw head for scope %q: %w", req.Scope, rerr)
		}
		if raw < pos {
			pos = head
			break
		}
		if werr := s.wait(ctx); werr != nil {
			return werr
		}
		if head, err = log.Head(ctx); err != nil {
			return fmt.Errorf("feed: head for scope %q: %w", req.Scope, err)
		}
		// The re-head is the probe point: a clamp that persists here is
		// a wedged fence, and the wait is observable.
		probeClamp()
	}

	for {
		// One loop iteration's start: the idle wait's floor measures the
		// page read, the sends and the wait against Poll from here, so an
		// instantly-returning Waiter still yields one page read per poll.
		iterStart := time.Now()
		if err := ctx.Err(); err != nil {
			return err
		}
		envs, truncated, err := log.ReadAllBounded(ctx, pos, batch, s.cfg.MaxBatchBytes, req.Definition.Consumes)
		if err != nil {
			// The adapters' poison contract: a first row over the
			// store's cap surfaces as *EventTooLargeError without the
			// payload ever being materialized (spec §5.3).
			var poison *EventTooLargeError
			if errors.As(err, &poison) {
				s.cfg.Observer.OnPoison(poison)
			}
			return fmt.Errorf("feed: read %s/%s/%s from %d: %w", req.Scope, name, req.Instance, pos, err)
		}
		pageBytes := 0
		for _, env := range envs {
			pageBytes += len(env.Payload)
		}
		s.cfg.Observer.OnPage(len(envs), pageBytes)
		for _, env := range envs {
			if s.cfg.MaxPayloadBytes > 0 && len(env.Payload) > s.cfg.MaxPayloadBytes {
				// The server's own backstop check: the row was
				// materialized, but the stream refuses to send it.
				poison := &EventTooLargeError{
					Position: env.GlobalPosition,
					TypeURL:  env.TypeURL,
					Size:     len(env.Payload),
					Limit:    s.cfg.MaxPayloadBytes,
				}
				s.cfg.Observer.OnPoison(poison)
				return poison
			}
			if err := sink.Send(ctx, env); err != nil {
				return fmt.Errorf("feed: send event at position %d: %w", env.GlobalPosition, err)
			}
			pos = env.GlobalPosition
		}
		if len(envs) > 0 {
			// A delivered page ended any stall episode: the next
			// one reports again.
			stallReported = false
			// A budget-truncated page, or a full page, means more matching
			// events are already there: continue without waiting. Guard on
			// a non-empty page: an adapter that reports truncated with zero
			// rows must not make the loop spin — wait like any other empty
			// page.
			if truncated || len(envs) >= batch {
				continue
			}
		}
		// The log has nothing more to deliver now: probe for a stall —
		// committed backlog the settled reads cannot deliver — then wait
		// out the poll.
		probeStall()
		if err := s.idleWait(ctx, iterStart); err != nil {
			return err
		}
	}
}

// wait idles until the next poll, or until the Waiter reports new events.
// The fence-clamp loop's re-head retry uses it: an unjittered, unfloored
// pause is the right shape there — a bounded retry cadence, not the idle
// poll. The read loop's idle branch uses idleWait instead.
func (s *Server) wait(ctx context.Context) error {
	if s.cfg.Waiter != nil {
		return s.cfg.Waiter.Wait(ctx, s.cfg.Poll)
	}
	t := time.NewTimer(s.cfg.Poll)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// idleWait waits out one idle poll — the empty-or-short-page branch of the
// read loop — and is the only wait that jitters (spec §5.12): the poll is
// scaled by the jitter source, so a fleet of streams started together
// de-synchronizes instead of polling in lockstep and micro-bursting the
// connection pool. The factor is drawn per idle wait, so each stream's phase
// random-walks away from the fleet's.
//
// The Waiter path is floored: Wait returning nil means "timeout" by the
// Waiter contract, so a Waiter whose implementation returns nil immediately
// must not turn the loop into a spin against the log. Whatever the Waiter
// did, the loop iteration that began at iterStart has spent at least Poll by
// the time idleWait returns — the server sleeps the remainder. The timer
// path needs no floor: the timer itself waits the jittered poll out, and
// sleeping a remainder on top would both double-sleep and cancel the
// sub-Poll half of the jitter.
func (s *Server) idleWait(ctx context.Context, iterStart time.Time) error {
	d := s.cfg.Poll
	if s.jitter != nil {
		d = s.jitter(d)
	}
	if s.cfg.Waiter == nil {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			return nil
		}
	}
	if err := s.cfg.Waiter.Wait(ctx, d); err != nil {
		return err
	}
	// The floor: at least Poll since the iteration began. Cancellation
	// still ends the stream promptly — the floor never blocks shutdown.
	if rem := s.cfg.Poll - time.Since(iterStart); rem > 0 {
		t := time.NewTimer(rem)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	return nil
}

// batchSize resolves Request.BatchSize against Config.Batch and MaxBatch.
func (s *Server) batchSize(requested int) int {
	if requested <= 0 {
		requested = s.cfg.Batch
	}
	if requested > s.cfg.MaxBatch {
		requested = s.cfg.MaxBatch
	}
	if requested < 1 {
		requested = 1
	}
	return requested
}

// Checkpoint records the consumer's acknowledged position — the only writer
// of the delivery cursor; Subscribe never saves. The ack is fenced on the
// lease: the subscription key must be held by ack.Owner, so a stale
// consumer's late ack (an in-flight receipt arriving after a takeover) or a
// foreign owner's claim is rejected instead of advancing the shared cursor
// past events the live subscriber has not delivered. Not held rejects with
// an error matching ErrLeaseLost; a different holder with *LeaseHeldError.
//
// The holder check and the save are two steps, and a lease may be lost
// between them. The failure mode stays safe: the ack is rejected and the
// consumer reconnects — never an advanced cursor. A stream subscribed with
// an empty Request.Owner has a server-minted, opaque owner and therefore
// cannot ack. ack.Definition is the checkpoint's identity when non-empty:
// it is persisted with the position, and an empty Definition preserves the
// stored fingerprint.
func (s *Server) Checkpoint(ctx context.Context, ack Ack) error {
	if err := validateKey(ack.Scope, ack.Projection, ack.Instance); err != nil {
		return err
	}
	key := fmt.Sprintf("%s/%s/%s", ack.Scope, ack.Projection, ack.Instance)
	lease, held, err := s.cfg.Leases.Holder(ctx, ack.Scope, ack.Projection, ack.Instance)
	if err != nil {
		return fmt.Errorf("feed: holder for %s: %w", key, err)
	}
	if !held {
		return &LeaseLostError{Key: key, Owner: ack.Owner, Err: errLeaseExpired}
	}
	if lease.Owner != ack.Owner {
		return &LeaseHeldError{Key: key, Lease: lease, Held: true}
	}
	if err := s.cfg.Checkpoints.Save(ctx, ack.Scope, ack.Projection, ack.Instance, ack.Definition, ack.Position); err != nil {
		return fmt.Errorf("feed: save checkpoint %s at %d: %w", key, ack.Position, err)
	}
	s.cfg.Observer.OnCheckpoint(ack.Position)
	return nil
}

// ResetCheckpoint rewinds or advances the checkpoint and re-identifies it
// under req's definition: the stored fingerprint becomes
// req.Definition.Hash(), so a subscription under a changed definition can
// serve again — a reset is a deliberate re-subscription under the new
// identity, and it is the recovery for ErrDefinitionChanged. The
// subscription key and the definition are read from req exactly like a
// Subscribe call (Name must match Projection, Consumes must be
// non-empty); req's stream-only fields (Owner, Consumer, BatchSize) are
// ignored. Unless force is set it refuses while a lease is held.
// Authorizing force is the caller's job: the core only enforces the lease.
//
// Force moves the cursor but does not stop the stream it displaces: a live
// subscriber under the old definition still holds the lease, and its next
// ack — fenced on ownership, which it still passes — re-records the old
// definition fingerprint and its own position, flipping the reset back. A
// forced reset sticks only after the old stream ends; stop it first when
// the reset must hold.
func (s *Server) ResetCheckpoint(ctx context.Context, req Request, position uint64, force bool) error {
	if err := validateKey(req.Scope, req.Projection, req.Instance); err != nil {
		return err
	}
	if err := validateDefinition(req); err != nil {
		return err
	}
	if !force {
		if err := s.refuseIfHeld(ctx, req.Scope, req.Projection, req.Instance); err != nil {
			return err
		}
	}
	if err := s.cfg.Checkpoints.Reset(ctx, req.Scope, req.Projection, req.Instance, req.Definition.Hash(), position); err != nil {
		return fmt.Errorf("feed: reset checkpoint %s/%s/%s to %d: %w", req.Scope, req.Projection, req.Instance, position, err)
	}
	return nil
}

// Skip deliberately advances a subscription's checkpoint to sr.Position —
// resume reads strictly after it — and records the decision as a dead
// letter: the operator's audited recovery path for a poison event, the
// stream-killing payload a subscription can never deliver (a
// *EventTooLargeError). It completes the poison path: the append cap stops
// such events entering the log, the adapters never materialize them, and
// Skip is how an operator deliberately moves past one.
//
// Skip is a position advance, never a redefinition: it preserves the
// checkpoint's stored definition fingerprint, so the next Subscribe serves
// under the identity the skipped history was acknowledged under. The dead
// letter is a pointer, not a copy: the event stays in the immutable log,
// and the runbook is triage by (scope, projection, reason) — fix the
// cause, then either accept the gap as a recorded omission or replay it:
// ResetCheckpoint to a position before the skip re-delivers the
// dead-lettered event like any other.
//
// Unless sr.Force is set, Skip refuses while a live lease holds the key —
// the same fence as ResetCheckpoint, so a running subscriber's cursor is
// never moved underneath it. The normal flow needs no force: the poison
// error already killed the stream and released its lease.
//
// Skip does not require the position to exist or the event to be over-cap;
// it is a deliberate admin cursor-advance, and validateKey gates the
// subscription identity.
//
// When Config.DeadLetters is non-nil, the dead letter is recorded BEFORE
// the checkpoint advances: a crash between the two leaves an audit row
// and an unadvanced cursor — safe, the skip is simply retried (the
// store's insert-or-replace keeps the retry idempotent) — while the
// reverse order could advance the cursor with no audit record at all. A
// failed record refuses the skip: the cursor never advances unaudited.
func (s *Server) Skip(ctx context.Context, sr SkipRequest) error {
	if err := validateKey(sr.Scope, sr.Projection, sr.Instance); err != nil {
		return err
	}
	if !sr.Force {
		if err := s.refuseIfHeld(ctx, sr.Scope, sr.Projection, sr.Instance); err != nil {
			return err
		}
	}
	// Load the stored fingerprint and pass it back through Reset: a skip
	// preserves the checkpoint's identity. An absent row carries the
	// empty hash, which Reset's preserve-on-empty contract keeps unset.
	cp, _, err := s.cfg.Checkpoints.Checkpoint(ctx, sr.Scope, sr.Projection, sr.Instance)
	if err != nil {
		return fmt.Errorf("feed: load checkpoint %s/%s/%s: %w", sr.Scope, sr.Projection, sr.Instance, err)
	}
	// Record the audit row before advancing: a crash between the two
	// leaves a dead letter and an unadvanced cursor — safe, the skip is
	// retried — while the reverse order could advance with no record at
	// all. A failed record refuses the skip: never advance unaudited.
	if s.cfg.DeadLetters != nil {
		dl := DeadLetter{
			Scope:      sr.Scope,
			Projection: sr.Projection,
			Instance:   sr.Instance,
			Position:   sr.Position,
			Reason:     sr.Reason,
			Actor:      sr.Actor,
			CreatedAt:  s.cfg.Clock.Now(),
		}
		if err := s.cfg.DeadLetters.Record(ctx, dl); err != nil {
			return fmt.Errorf("feed: record dead letter %s/%s/%s at %d: %w",
				sr.Scope, sr.Projection, sr.Instance, sr.Position, err)
		}
	}
	if err := s.cfg.Checkpoints.Reset(ctx, sr.Scope, sr.Projection, sr.Instance, cp.DefinitionHash, sr.Position); err != nil {
		return fmt.Errorf("feed: skip checkpoint %s/%s/%s to %d: %w", sr.Scope, sr.Projection, sr.Instance, sr.Position, err)
	}
	return nil
}

// DeleteSubscription removes the checkpoint. It refuses while a lease is held;
// releasing leases (including expired ones) is the store adapter's job.
func (s *Server) DeleteSubscription(ctx context.Context, scope, projection, instance string) error {
	if err := validateKey(scope, projection, instance); err != nil {
		return err
	}
	if err := s.refuseIfHeld(ctx, scope, projection, instance); err != nil {
		return err
	}
	if err := s.cfg.Checkpoints.Delete(ctx, scope, projection, instance); err != nil {
		return fmt.Errorf("feed: delete checkpoint %s/%s/%s: %w", scope, projection, instance, err)
	}
	return nil
}

// Status reports the subscription's observable state: the stored
// checkpoint, the matching head and lag for req's definition, and the
// active lease holder. req identifies the subscription exactly like a
// Subscribe call and is validated the same way; the checkpoint is read
// once. Head is MatchingHead over req.Definition.Consumes — the highest
// settled position among the subscription's own event types — and Lag is
// matchingHead − position, negative when the checkpoint is ahead of a
// restored log, and transiently negative on a fence-bounded Log (Postgres,
// ADR 0037) while a blocked fence settles MatchingHead to 0. Status never
// fails on definition drift: it observes, it does not gate.
func (s *Server) Status(ctx context.Context, req Request) (Status, error) {
	if err := validateKey(req.Scope, req.Projection, req.Instance); err != nil {
		return Status{}, err
	}
	if err := validateDefinition(req); err != nil {
		return Status{}, err
	}
	cp, found, err := s.cfg.Checkpoints.Checkpoint(ctx, req.Scope, req.Projection, req.Instance)
	if err != nil {
		return Status{}, fmt.Errorf("feed: checkpoint %s/%s/%s: %w", req.Scope, req.Projection, req.Instance, err)
	}
	log, err := s.cfg.Logs.Open(ctx, req.Scope)
	if err != nil {
		return Status{}, fmt.Errorf("feed: open log for scope %q: %w", req.Scope, err)
	}
	head, err := log.MatchingHead(ctx, req.Definition.Consumes)
	if err != nil {
		return Status{}, fmt.Errorf("feed: matching head for scope %q: %w", req.Scope, err)
	}
	var updated time.Time
	if found {
		updated = cp.UpdatedAt
	}
	st := Status{
		Position:         cp.Position,
		Head:             head,
		Lag:              int64(head) - int64(cp.Position),
		LastCheckpointAt: updated,
	}
	if lease, held, err := s.cfg.Leases.Holder(ctx, req.Scope, req.Projection, req.Instance); err != nil {
		return Status{}, fmt.Errorf("feed: holder for %s/%s/%s: %w", req.Scope, req.Projection, req.Instance, err)
	} else if held {
		st.ActiveConsumer = lease.Consumer
	}
	return st, nil
}

// refuseIfHeld returns a *LeaseHeldError when the subscription key has a live
// holder.
func (s *Server) refuseIfHeld(ctx context.Context, scope, projection, instance string) error {
	lease, held, err := s.cfg.Leases.Holder(ctx, scope, projection, instance)
	if err != nil {
		return fmt.Errorf("feed: holder for %s/%s/%s: %w", scope, projection, instance, err)
	}
	if held {
		return &LeaseHeldError{Key: fmt.Sprintf("%s/%s/%s", scope, projection, instance), Lease: lease, Held: true}
	}
	return nil
}

// validateRequest checks the fields Subscribe cannot infer. Definition must
// carry a name and at least one consumed type: the caller resolves and
// authorizes it before subscribing, and an empty definition would seal the
// stream silently. Projection must equal Definition.Name — they name the same
// projection, and a mismatch would stream one projection's type set but file
// its checkpoints under another projection's name.
func validateRequest(req Request, sink Sink) error {
	if err := validateKey(req.Scope, req.Projection, req.Instance); err != nil {
		return err
	}
	if err := validateDefinition(req); err != nil {
		return err
	}
	if sink == nil {
		return fmt.Errorf("%w: sink is required", ErrInvalidRequest)
	}
	return nil
}

// validateDefinition checks the definition every definition-carrying call
// must pass: a name that matches the projection, at least one consumed
// type, and no control byte in any consumed type URL. Subscribe resolves
// and authorizes the type set before calling the core; an empty definition
// would seal the stream silently, and a mismatched name would file one
// projection's checkpoints under another's. The control-byte refusal is
// the same byte-level hygiene as the subscription key (containsControl):
// a control byte is never legitimate in a type URL, and one accepted here
// reaches every adapter's type filter and every matching-head probe the
// core runs over the type set.
func validateDefinition(req Request) error {
	if req.Definition.Name == "" {
		return fmt.Errorf("%w: definition name is required", ErrInvalidRequest)
	}
	if req.Definition.Name != req.Projection {
		return fmt.Errorf("%w: projection %q does not match definition name %q",
			ErrInvalidRequest, req.Projection, req.Definition.Name)
	}
	if len(req.Definition.Consumes) == 0 {
		return fmt.Errorf("%w: definition consumes is required", ErrInvalidRequest)
	}
	for _, typ := range req.Definition.Consumes {
		if containsControl(typ) {
			return fmt.Errorf("%w: consumed type %q contains a control byte", ErrInvalidRequest, typ)
		}
	}
	return nil
}

// containsControl reports whether s contains an ASCII control byte (C0
// controls or DEL). Adapters may join the key parts into one storage key
// (the sqlite adapter joins them with NUL), so a control byte inside any
// part would let distinct triples collide into one key.
func containsControl(s string) bool {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < 0x20 || c == 0x7f {
			return true
		}
	}
	return false
}

// validateKey checks one subscription identity. Scope and projection are
// required; instance may be empty (the documented default instance). Control
// bytes are refused in every part: adapters may join the parts into one
// storage key with NUL, so a NUL inside a part would make distinct triples
// collide.
func validateKey(scope, projection, instance string) error {
	switch {
	case scope == "":
		return fmt.Errorf("%w: scope is required", ErrInvalidRequest)
	case projection == "":
		return fmt.Errorf("%w: projection is required", ErrInvalidRequest)
	case containsControl(scope):
		return fmt.Errorf("%w: scope contains a control byte", ErrInvalidRequest)
	case containsControl(projection):
		return fmt.Errorf("%w: projection contains a control byte", ErrInvalidRequest)
	case containsControl(instance):
		return fmt.Errorf("%w: instance contains a control byte", ErrInvalidRequest)
	}
	return nil
}
