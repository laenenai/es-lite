package feed

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
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
	// Clock supplies wall time for lease owner identity; nil is time.Now.
	Clock Clock

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
	// Poll is the idle wait between short pages. Default 250ms.
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
	return &Server{cfg: cfg, prefix: prefix}, nil
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// randRead supplies newPrefix's entropy. It is a variable so the internal
// test can force an entropy failure.
var randRead = rand.Read

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
// Subscribe never saves checkpoints. The consumer acknowledges with
// Checkpoint, so reconnecting resumes at its last acknowledged batch.
func (s *Server) Subscribe(ctx context.Context, req Request, sink Sink) (err error) {
	if err := validateRequest(req, sink); err != nil {
		return err
	}
	key := LeaseKey(req.Scope, req.Projection, req.Instance)
	owner := s.newOwner()

	acquired, err := s.cfg.Leases.Acquire(ctx, key, owner, req.Consumer, s.cfg.LeaseTTL)
	if err != nil {
		aerr := fmt.Errorf("feed: acquire lease %q: %w", key, err)
		// A store may report an error after it took the lease (a failed
		// post-commit step, for example). (true, err) means we own it, so
		// release it rather than leaking the key.
		if acquired {
			if rerr := s.releaseLease(ctx, key, owner); rerr != nil {
				aerr = errors.Join(aerr, fmt.Errorf("feed: release lease %q after acquire error: %w", key, rerr))
			}
		}
		return aerr
	}
	if !acquired {
		lease, held, herr := s.cfg.Leases.Holder(ctx, key)
		if herr != nil {
			return fmt.Errorf("feed: acquire lease %q: %w", key, herr)
		}
		return &LeaseHeldError{Key: key, Lease: lease, Held: held}
	}

	// runCtx is cancelled by the caller's ctx or by a failed renewal, so the
	// read loop and its sink both stop promptly.
	runCtx, cancel := context.WithCancel(ctx)
	lost := make(chan error, 1)
	var renewals sync.WaitGroup
	renewals.Add(1)
	go func() {
		defer renewals.Done()
		s.renew(runCtx, key, owner, lost, cancel)
	}()

	// Release ordering: stop the stream, wait for the renewer to exit, then
	// release. A Renew can therefore never race the Release and resurrect the
	// lease. The release uses a context detached from the caller's, because
	// cancellation must not leak the lease.
	defer func() {
		cancel()
		renewals.Wait()
		if rerr := s.releaseLease(ctx, key, owner); rerr != nil {
			err = errors.Join(err, fmt.Errorf("feed: release lease %q: %w", key, rerr))
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

// releaseLease releases key for owner on a context detached from the stream's
// cancellation, bounded by leaseReleaseTimeout, so a cancelled stream still
// frees its key and values (tenant scope, tracing) survive.
func (s *Server) releaseLease(ctx context.Context, key, owner string) error {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), leaseReleaseTimeout)
	defer cancel()
	return s.cfg.Leases.Release(releaseCtx, key, owner)
}

// renew extends the lease every RenewEvery until ctx ends. On the first failed
// renewal it records a *LeaseLostError and cancels the stream. The error is
// buffered and recorded before the cancel, so Subscribe can always observe it.
func (s *Server) renew(ctx context.Context, key, owner string, lost chan<- error, cancel context.CancelFunc) {
	ticker := time.NewTicker(s.cfg.RenewEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		ok, rerr := s.cfg.Leases.Renew(ctx, key, owner, s.cfg.LeaseTTL)
		if ctx.Err() != nil {
			return // cancellation (caller or our own) ended the stream
		}
		if ok && rerr == nil {
			continue
		}
		if rerr == nil {
			rerr = errLeaseExpired
		}
		select {
		case lost <- &LeaseLostError{Key: key, Owner: owner, Err: rerr}:
		default:
		}
		cancel()
		return
	}
}

// stream is the read/send loop. It runs under runCtx, which the renewer may
// cancel; Subscribe translates the result. The event type set is the
// request's authorized Definition.Consumes, never a caller-built list.
func (s *Server) stream(ctx context.Context, req Request, sink Sink, batch int) error {
	name := req.Definition.Name
	log, err := s.cfg.Logs.Open(ctx, req.Scope)
	if err != nil {
		return fmt.Errorf("feed: open log for scope %q: %w", req.Scope, err)
	}
	pos, err := s.cfg.Checkpoints.Load(ctx, req.Scope, req.Projection, req.Instance)
	if err != nil {
		return fmt.Errorf("feed: load checkpoint %s/%s/%s: %w", req.Scope, name, req.Instance, err)
	}
	head, err := log.Head(ctx)
	if err != nil {
		return fmt.Errorf("feed: head for scope %q: %w", req.Scope, err)
	}
	// A log restored to an earlier point leaves the checkpoint ahead of the
	// head. Clamp locally; do not stall on a position the log no longer has
	// and do not rewrite the consumer's checkpoint.
	if pos > head {
		pos = head
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		envs, truncated, err := log.ReadAllBounded(ctx, pos, batch, s.cfg.MaxBatchBytes, req.Definition.Consumes)
		if err != nil {
			return fmt.Errorf("feed: read %s/%s/%s from %d: %w", req.Scope, name, req.Instance, pos, err)
		}
		for _, env := range envs {
			if s.cfg.MaxPayloadBytes > 0 && len(env.Payload) > s.cfg.MaxPayloadBytes {
				return &EventTooLargeError{
					Position: env.GlobalPosition,
					TypeURL:  env.TypeURL,
					Size:     len(env.Payload),
					Limit:    s.cfg.MaxPayloadBytes,
				}
			}
			if err := sink.Send(ctx, env); err != nil {
				return fmt.Errorf("feed: send event at position %d: %w", env.GlobalPosition, err)
			}
			pos = env.GlobalPosition
		}
		// A budget-truncated page, or a full page, means more matching events
		// are already there: continue without waiting. Guard on a non-empty
		// page: an adapter that reports truncated with zero rows must not make
		// the loop spin — wait like any other empty page.
		if len(envs) > 0 && (truncated || len(envs) >= batch) {
			continue
		}
		if err := s.wait(ctx); err != nil {
			return err
		}
	}
}

// wait idles until the next poll, or until the Waiter reports new events.
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

// Checkpoint records the consumer's acknowledged position. The consumer calls
// this through the transport adapter; Subscribe itself never saves.
func (s *Server) Checkpoint(ctx context.Context, scope, projection, instance string, position uint64) error {
	if err := validateKey(scope, projection, instance); err != nil {
		return err
	}
	if err := s.cfg.Checkpoints.Save(ctx, scope, projection, instance, position); err != nil {
		return fmt.Errorf("feed: save checkpoint %s/%s/%s at %d: %w", scope, projection, instance, position, err)
	}
	return nil
}

// ResetCheckpoint rewinds or advances the checkpoint. Unless force is set it
// refuses while a lease is held. Authorizing force is the caller's job: the
// core only enforces the lease.
func (s *Server) ResetCheckpoint(ctx context.Context, scope, projection, instance string, position uint64, force bool) error {
	if err := validateKey(scope, projection, instance); err != nil {
		return err
	}
	if !force {
		if err := s.refuseIfHeld(ctx, scope, projection, instance); err != nil {
			return err
		}
	}
	if err := s.cfg.Checkpoints.Reset(ctx, scope, projection, instance, position); err != nil {
		return fmt.Errorf("feed: reset checkpoint %s/%s/%s to %d: %w", scope, projection, instance, position, err)
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

// Status reports the stored checkpoint, the log head, positional lag and the
// active lease holder. Lag is head - position, and is negative when the
// checkpoint is ahead of a restored log.
func (s *Server) Status(ctx context.Context, scope, projection, instance string) (Status, error) {
	if err := validateKey(scope, projection, instance); err != nil {
		return Status{}, err
	}
	pos, err := s.cfg.Checkpoints.Load(ctx, scope, projection, instance)
	if err != nil {
		return Status{}, fmt.Errorf("feed: load checkpoint %s/%s/%s: %w", scope, projection, instance, err)
	}
	var updated time.Time
	if cp, found, err := s.cfg.Checkpoints.Checkpoint(ctx, scope, projection, instance); err != nil {
		return Status{}, fmt.Errorf("feed: checkpoint %s/%s/%s: %w", scope, projection, instance, err)
	} else if found {
		pos, updated = cp.Position, cp.UpdatedAt
	}
	log, err := s.cfg.Logs.Open(ctx, scope)
	if err != nil {
		return Status{}, fmt.Errorf("feed: open log for scope %q: %w", scope, err)
	}
	head, err := log.Head(ctx)
	if err != nil {
		return Status{}, fmt.Errorf("feed: head for scope %q: %w", scope, err)
	}
	st := Status{
		Position:         pos,
		Head:             head,
		Lag:              int64(head) - int64(pos),
		LastCheckpointAt: updated,
	}
	if lease, held, err := s.cfg.Leases.Holder(ctx, LeaseKey(scope, projection, instance)); err != nil {
		return Status{}, fmt.Errorf("feed: holder for %s/%s/%s: %w", scope, projection, instance, err)
	} else if held {
		st.ActiveConsumer = lease.Consumer
	}
	return st, nil
}

// refuseIfHeld returns a *LeaseHeldError when the subscription key has a live
// holder.
func (s *Server) refuseIfHeld(ctx context.Context, scope, projection, instance string) error {
	key := LeaseKey(scope, projection, instance)
	lease, held, err := s.cfg.Leases.Holder(ctx, key)
	if err != nil {
		return fmt.Errorf("feed: holder for %s/%s/%s: %w", scope, projection, instance, err)
	}
	if held {
		return &LeaseHeldError{Key: key, Lease: lease, Held: true}
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
	if sink == nil {
		return fmt.Errorf("%w: sink is required", ErrInvalidRequest)
	}
	return nil
}

// containsControl reports whether s contains an ASCII control byte (C0
// controls or DEL). LeaseKey joins the key parts with NUL, so a control byte
// inside any part would let distinct triples collide into one key.
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
// bytes are refused in every part: LeaseKey joins the parts with NUL, so a
// NUL inside a part would make distinct triples collide.
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
