package feed

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/es-lite/es"
)

// ChangeLog is the unfiltered, commit-safe log read a change feed needs.
// It is deliberately narrower than Log: a change feed consumes every
// event in the workspace, not a projection's filtered type set, so it has
// no definition to hash or manifest to maintain. *postgres.Workspace
// implements it with the same horizon fence as ReadAllBounded: a page
// never contains a live hole, so the checkpoint can never pass an event
// that commits later. The raw, unfenced Workspace.ReadAll is not
// sufficient and must not be used here.
//
// The workspace must be unsealed: Keys receives the stored payload, and a
// sealed workspace stores ciphertext. A sealed deployment that needs
// signals derives keys in the sealing adapter instead.
type ChangeLog interface {
	// ReadAllSettled returns up to limit events with GlobalPosition >
	// from, ordered ascending, bounded by the settled horizon and by
	// maxBytes of stored payload. truncated reports that the budget
	// stopped the page and more events remain, so the caller reads again
	// instead of waiting.
	ReadAllSettled(ctx context.Context, from uint64, limit, maxBytes int) ([]es.Envelope, bool, error)
	// Head returns the settled log head. A checkpoint above it is either
	// a restored log or a transiently blocked fence.
	Head(ctx context.Context) (uint64, error)
	// RawHead returns the raw committed maximum, without the settle
	// bound: raw < checkpoint proves the checkpoint's position is gone.
	RawHead(ctx context.Context) (uint64, error)
}

// ChangeFeedConfig configures one change feed. A change feed reads the
// unfiltered log from its own checkpoint, maps each envelope to keys,
// publishes a change set, and advances. It is best-effort: a publish
// failure is reported and dropped, and a crash between publishing and
// advancing may duplicate a change set, which the hub coalesces.
//
// The workspace must be unsealed: Keys receives the stored payload, and a
// sealed workspace stores ciphertext.
type ChangeFeedConfig struct {
	// Scope is the workspace id. Required.
	Scope string
	// Name is the subscription key (for example "iam.changes") and the
	// checkpoint identity. Required.
	Name string
	// Instance defaults to "live".
	Instance string
	// Consumer names the worker in the lease row, for Status. Defaults
	// to Name.
	Consumer string

	// Log, Leases and Checkpoints are required.
	Log         ChangeLog
	Leases      LeaseStore
	Checkpoints CheckpointStore

	// Keys and Publish are required. Keys returns the keys an event
	// touched, in the order they should be published; it owns any
	// de-duplication. An empty result publishes nothing.
	Keys    func(es.Envelope) []string
	Publish func(ctx context.Context, keys []string, position uint64) error

	// OnError receives a publish failure. It must not block; nil logs a
	// warning. The feed advances regardless: a failed hint is dropped.
	OnError func(error)

	// Waiter is the optional wake source. A nil Waiter polls every Poll.
	// It must follow the Waiter contract and return false on a timeout: a
	// waiter that always returns true spins this loop, and the feed does
	// not floor a wake because a genuine wake must read immediately.
	Waiter Waiter

	// Poll is the idle wait and the fallback clock. Default 1 s.
	Poll time.Duration
	// LeaseTTL is the lease lifetime. Default 30 s.
	LeaseTTL time.Duration
	// RenewEvery is the lease renewal interval. Default 10 s, kept below
	// LeaseTTL.
	RenewEvery time.Duration
	// Batch is the page size. Default 500.
	Batch int
	// MaxBatchBytes bounds one page's stored payload bytes. Default
	// 16 MiB; a truncated page is read again immediately. The first event
	// of a page is always included, so an oversized event is visible to
	// Keys rather than silently skipped.
	MaxBatchBytes int
}

// ChangeFeed is a leased, checkpointed reader over the unfiltered log.
// One instance runs per (scope, name, instance) key; standbys retry with
// backoff and take over when the lease is released or expires.
type ChangeFeed struct {
	cfg ChangeFeedConfig
}

// NewChangeFeed validates cfg, applies defaults, and returns a feed that
// has not touched the database. Run does the I/O.
func NewChangeFeed(cfg ChangeFeedConfig) (*ChangeFeed, error) {
	if cfg.Scope == "" {
		return nil, fmt.Errorf("%w: change feed scope is required", ErrInvalidRequest)
	}
	if cfg.Name == "" {
		return nil, fmt.Errorf("%w: change feed name is required", ErrInvalidRequest)
	}
	if cfg.Log == nil || cfg.Leases == nil || cfg.Checkpoints == nil {
		return nil, fmt.Errorf("%w: change feed log, leases and checkpoints are required", ErrInvalidRequest)
	}
	if cfg.Keys == nil || cfg.Publish == nil {
		return nil, fmt.Errorf("%w: change feed keys and publish are required", ErrInvalidRequest)
	}
	if cfg.Instance == "" {
		cfg.Instance = "live"
	}
	if cfg.Consumer == "" {
		cfg.Consumer = cfg.Name
	}
	if cfg.Poll <= 0 {
		cfg.Poll = time.Second
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 30 * time.Second
	}
	if cfg.RenewEvery <= 0 {
		cfg.RenewEvery = 10 * time.Second
	}
	if cfg.RenewEvery >= cfg.LeaseTTL {
		cfg.RenewEvery = cfg.LeaseTTL / 3
		if cfg.RenewEvery <= 0 {
			return nil, fmt.Errorf("%w: change feed LeaseTTL %v is too small to renew safely", ErrInvalidRequest, cfg.LeaseTTL)
		}
	}
	if cfg.Batch <= 0 {
		cfg.Batch = 500
	}
	if cfg.MaxBatchBytes <= 0 {
		cfg.MaxBatchBytes = 16 << 20
	}
	if cfg.OnError == nil {
		cfg.OnError = func(err error) {
			slog.Warn("feed: publish change set", "err", err)
		}
	}
	return &ChangeFeed{cfg: cfg}, nil
}

// Run acquires the lease, then reads and publishes until ctx ends or the
// lease is lost. A held lease is retried with jittered backoff; the
// caller does not implement its own standby loop.
func (f *ChangeFeed) Run(ctx context.Context) error {
	owner := uuid.NewString()
	if err := f.acquire(ctx, owner); err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	lost := make(chan error, 1)
	var renewals sync.WaitGroup
	renewals.Add(1)
	go func() {
		defer renewals.Done()
		f.renew(runCtx, owner, lost, cancel)
	}()

	pos, err := f.cfg.Checkpoints.Load(runCtx, f.cfg.Scope, f.cfg.Name, f.cfg.Instance)
	if err != nil {
		cancel()
		renewals.Wait()
		f.release(ctx, owner)
		return fmt.Errorf("feed: change feed load checkpoint: %w", err)
	}
	pos, err = f.align(runCtx, pos)
	if err != nil {
		cancel()
		renewals.Wait()
		f.release(ctx, owner)
		return err
	}

	streamErr := f.stream(runCtx, pos)
	cancel()
	renewals.Wait()

	var lostErr error
	select {
	case lostErr = <-lost:
	default:
	}
	f.release(ctx, owner)

	if lostErr != nil {
		return lostErr
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return streamErr
}

func (f *ChangeFeed) release(ctx context.Context, owner string) {
	// Detached from cancellation, but bounded: a hung Release must not
	// block shutdown forever.
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := f.cfg.Leases.Release(releaseCtx, f.cfg.Scope, f.cfg.Name, f.cfg.Instance, owner); err != nil {
		slog.Warn("feed: change feed release lease", "err", err)
	}
}

// acquire retries a held lease with the same schedule as the projection
// reconnect loop: 250 ms doubling to 30 s, with jitter.
func (f *ChangeFeed) acquire(ctx context.Context, owner string) error {
	delay := 250 * time.Millisecond
	const maxDelay = 30 * time.Second
	for {
		ok, err := f.cfg.Leases.Acquire(ctx, f.cfg.Scope, f.cfg.Name, f.cfg.Instance, owner, f.cfg.Consumer, f.cfg.LeaseTTL)
		if err != nil {
			// A store may report an error after taking the lease; release
			// it rather than leaving the key held until its TTL.
			if ok {
				f.release(ctx, owner)
			}
			return fmt.Errorf("feed: change feed acquire lease: %w", err)
		}
		if ok {
			return nil
		}
		timer := time.NewTimer(defaultJitter(delay))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}
}

// renew extends the lease until ctx ends. The first failed renewal
// cancels the stream and reports a *LeaseLostError; another subscriber
// may already own the key, so continuing would risk two publishers.
func (f *ChangeFeed) renew(ctx context.Context, owner string, lost chan<- error, cancel context.CancelFunc) {
	ticker := time.NewTicker(f.cfg.RenewEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, err := f.cfg.Leases.Renew(ctx, f.cfg.Scope, f.cfg.Name, f.cfg.Instance, owner, f.cfg.LeaseTTL)
			if ctx.Err() != nil {
				// A shutdown cancelling an in-flight Renew is not a loss.
				return
			}
			if err == nil && ok {
				continue
			}
			if err == nil {
				err = errLeaseExpired
			}
			select {
			case lost <- &LeaseLostError{
				Key:   f.cfg.Scope + "/" + f.cfg.Name + "/" + f.cfg.Instance,
				Owner: owner,
				Err:   err,
			}:
			default:
			}
			cancel()
			return
		}
	}
}

// align recovers a checkpoint ahead of the log. A restored log leaves the
// checkpoint above the settled head; a blocked fence reports a transient
// low head with the raw position still present. RawHead separates them:
// raw < checkpoint means the position is gone, so reset the checkpoint to
// the settled head (the signal feed owns it) and continue; raw >=
// checkpoint means wait and re-head.
func (f *ChangeFeed) align(ctx context.Context, pos uint64) (uint64, error) {
	for {
		iterStart := time.Now()
		head, err := f.cfg.Log.Head(ctx)
		if err != nil {
			return 0, fmt.Errorf("feed: change feed head: %w", err)
		}
		if pos <= head {
			return pos, nil
		}
		raw, err := f.cfg.Log.RawHead(ctx)
		if err != nil {
			return 0, fmt.Errorf("feed: change feed raw head: %w", err)
		}
		if raw < pos {
			if err := f.cfg.Checkpoints.Reset(ctx, f.cfg.Scope, f.cfg.Name, f.cfg.Instance, "", head); err != nil {
				return 0, fmt.Errorf("feed: change feed reset restored checkpoint: %w", err)
			}
			slog.Warn("feed: change feed checkpoint ahead of the log; reset to the settled head",
				"scope", f.cfg.Scope, "name", f.cfg.Name, "from", pos, "to", head)
			return head, nil
		}
		woke, err := f.wait(ctx)
		if err != nil {
			return 0, err
		}
		if !woke {
			if err := f.floor(ctx, iterStart); err != nil {
				return 0, err
			}
		}
	}
}

func (f *ChangeFeed) stream(ctx context.Context, pos uint64) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		iterStart := time.Now()
		envs, truncated, err := f.cfg.Log.ReadAllSettled(ctx, pos, f.cfg.Batch, f.cfg.MaxBatchBytes)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("feed: change feed read: %w", err)
		}
		for _, e := range envs {
			keys := f.cfg.Keys(e)
			if len(keys) > 0 {
				if err := f.cfg.Publish(ctx, keys, e.GlobalPosition); err != nil {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					f.cfg.OnError(fmt.Errorf("feed: publish change set at %d: %w", e.GlobalPosition, err))
				}
			}
			pos = e.GlobalPosition
		}
		if len(envs) > 0 {
			if err := f.cfg.Checkpoints.Save(ctx, f.cfg.Scope, f.cfg.Name, f.cfg.Instance, "", pos); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("feed: change feed save checkpoint: %w", err)
			}
		}
		if len(envs) < f.cfg.Batch && !truncated {
			woke, err := f.wait(ctx)
			if err != nil {
				return err
			}
			if !woke {
				if err := f.floor(ctx, iterStart); err != nil {
					return err
				}
			}
		}
	}
}

// floor spends the remainder of Poll since since, so a waiter that returns
// a non-wake at once cannot spin the loop. A genuine wake never floors.
func (f *ChangeFeed) floor(ctx context.Context, since time.Time) error {
	rem := f.cfg.Poll - time.Since(since)
	if rem <= 0 {
		return nil
	}
	timer := time.NewTimer(rem)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// wait parks until a wake, the poll expires, or ctx ends. A wake is a
// latency hint: whether it fired or not, the next iteration reads. woke is
// false for a timeout (and for the no-waiter timer), so the caller can
// floor a non-wake.
func (f *ChangeFeed) wait(ctx context.Context) (bool, error) {
	if scoped, ok := f.cfg.Waiter.(ScopedWaiter); ok {
		return scoped.WaitScope(ctx, f.cfg.Scope, f.cfg.Poll)
	}
	if f.cfg.Waiter != nil {
		return f.cfg.Waiter.Wait(ctx, f.cfg.Poll)
	}
	timer := time.NewTimer(f.cfg.Poll)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-timer.C:
		return false, nil
	}
}
