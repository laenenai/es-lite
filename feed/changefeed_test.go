package feed_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/feed"
)

// changeLog is an unfiltered in-memory log: the change feed reads every
// event, not a projection's consumed types.
type changeLog struct {
	mu            sync.Mutex
	events        []es.Envelope
	reads         []uint64
	err           error
	truncateAfter int
}

func (l *changeLog) ReadAllSettled(ctx context.Context, from uint64, limit, maxBytes int) ([]es.Envelope, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.reads = append(l.reads, from)
	if l.err != nil {
		return nil, false, l.err
	}
	var out []es.Envelope
	for _, e := range l.events {
		if e.GlobalPosition <= from {
			continue
		}
		out = append(out, e)
		if len(out) == limit {
			break
		}
	}
	if l.truncateAfter > 0 && len(out) > l.truncateAfter {
		return out[:l.truncateAfter], true, nil
	}
	return out, false, nil
}

func (l *changeLog) append(envs ...es.Envelope) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, envs...)
}

func (l *changeLog) readCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.reads)
}

func (l *changeLog) readsSnapshot() []uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]uint64(nil), l.reads...)
}

func envelope(position uint64) es.Envelope {
	return es.Envelope{
		StreamID:       es.StreamID{Type: "tenant", ID: "t1"},
		GlobalPosition: position,
		TypeURL:        "test.Event",
	}
}

type published struct {
	keys     []string
	position uint64
}

type recordingPublisher struct {
	mu      sync.Mutex
	records []published
	errAt   map[uint64]error
}

func newPublisher() *recordingPublisher {
	return &recordingPublisher{errAt: map[uint64]error{}}
}

func (p *recordingPublisher) publish(_ context.Context, keys []string, position uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.records = append(p.records, published{keys: append([]string(nil), keys...), position: position})
	return p.errAt[position]
}

func (p *recordingPublisher) snapshot() []published {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]published(nil), p.records...)
}

func (p *recordingPublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.records)
}

// recordingWaiter parks until woken, the poll expires, or ctx ends. It
// records every timeout it was given so a test can pin the configured poll.
type recordingWaiter struct {
	mu       sync.Mutex
	timeouts []time.Duration
	wake     chan struct{}
}

func newWaiter() *recordingWaiter {
	return &recordingWaiter{wake: make(chan struct{}, 1)}
}

func (w *recordingWaiter) Wait(ctx context.Context, timeout time.Duration) (bool, error) {
	w.mu.Lock()
	w.timeouts = append(w.timeouts, timeout)
	w.mu.Unlock()
	select {
	case <-w.wake:
		return true, nil
	case <-ctx.Done():
		return false, ctx.Err()
	case <-time.After(timeout):
		return false, nil
	}
}

func (w *recordingWaiter) timeoutsSnapshot() []time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]time.Duration(nil), w.timeouts...)
}

func (w *recordingWaiter) signal() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func keysOf(e es.Envelope) []string {
	switch e.GlobalPosition {
	case 1:
		return []string{"tenant:t1"}
	case 3:
		return []string{"tenant:t1", "member:p1"}
	case 4:
		return []string{"member:p1"}
	}
	return nil
}

func baseConfig(log feed.ChangeLog, leases feed.LeaseStore, checkpoints feed.CheckpointStore, pub func(context.Context, []string, uint64) error) feed.ChangeFeedConfig {
	return feed.ChangeFeedConfig{
		Scope:       "s1",
		Name:        "iam.changes",
		Log:         log,
		Leases:      leases,
		Checkpoints: checkpoints,
		Keys:        keysOf,
		Publish:     pub,
		Poll:        5 * time.Millisecond,
		LeaseTTL:    time.Second,
		RenewEvery:  50 * time.Millisecond,
		Batch:       10,
	}
}

func runChangeFeed(t *testing.T, cfg feed.ChangeFeedConfig) (context.CancelFunc, <-chan error) {
	t.Helper()
	f, err := feed.NewChangeFeed(cfg)
	if err != nil {
		t.Fatalf("new change feed: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.Run(ctx) }()
	return cancel, done
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func waitCheckpoint(t *testing.T, checkpoints feed.CheckpointStore, want uint64) {
	t.Helper()
	waitFor(t, "the checkpoint", func() bool {
		got, err := checkpoints.Load(context.Background(), "s1", "iam.changes", "live")
		return err == nil && got == want
	})
}

func stop(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("change feed did not stop")
	}
}

func TestChangeFeedPublishesKeysAndPositions(t *testing.T) {
	log := &changeLog{}
	log.append(envelope(1), envelope(2), envelope(3), envelope(4))
	pub := newPublisher()
	checkpoints := newMemCheckpoints(time.Now())
	cancel, done := runChangeFeed(t, baseConfig(log, newMemLeases(), checkpoints, pub.publish))

	waitFor(t, "three published change sets", func() bool { return pub.count() >= 3 })
	waitCheckpoint(t, checkpoints, 4)
	stop(t, cancel, done)

	records := pub.snapshot()
	if len(records) != 3 {
		t.Fatalf("published %d change sets, want 3 (an empty key set publishes nothing)", len(records))
	}
	for i, want := range []uint64{1, 3, 4} {
		if records[i].position != want {
			t.Errorf("record %d position = %d, want %d", i, records[i].position, want)
		}
	}
	if len(records[1].keys) != 2 || records[1].keys[0] != "tenant:t1" || records[1].keys[1] != "member:p1" {
		t.Fatalf("record 1 keys = %v, want the tenant and member keys", records[1].keys)
	}
	if got, err := checkpoints.Load(context.Background(), "s1", "iam.changes", "live"); err != nil || got != 4 {
		t.Fatalf("checkpoint = %d, %v; want 4", got, err)
	}
}

func TestChangeFeedResumesFromItsCheckpoint(t *testing.T) {
	log := &changeLog{}
	log.append(envelope(1), envelope(2))
	leases := newMemLeases()
	checkpoints := newMemCheckpoints(time.Now())
	first := newPublisher()
	cancel, done := runChangeFeed(t, baseConfig(log, leases, checkpoints, first.publish))
	waitFor(t, "the first event", func() bool { return first.count() >= 1 })
	waitCheckpoint(t, checkpoints, 2)
	stop(t, cancel, done)

	log.append(envelope(3), envelope(4))
	second := newPublisher()
	cancel, done = runChangeFeed(t, baseConfig(log, leases, checkpoints, second.publish))
	waitFor(t, "the resumed events", func() bool { return second.count() >= 2 })
	stop(t, cancel, done)

	records := second.snapshot()
	if len(records) != 2 || records[0].position != 3 || records[1].position != 4 {
		t.Fatalf("resumed = %+v, want positions 3 and 4", records)
	}
}

func TestChangeFeedFencesWithTheLease(t *testing.T) {
	log := &changeLog{}
	log.append(envelope(1), envelope(3))
	leases := newMemLeases()
	checkpoints := newMemCheckpoints(time.Now())

	first := newPublisher()
	cancelFirst, doneFirst := runChangeFeed(t, baseConfig(log, leases, checkpoints, first.publish))
	waitFor(t, "the lease holder's first event", func() bool { return first.count() >= 1 })

	second := newPublisher()
	cancelSecond, doneSecond := runChangeFeed(t, baseConfig(log, leases, checkpoints, second.publish))
	time.Sleep(100 * time.Millisecond)
	if second.count() != 0 {
		t.Fatalf("the standby published %d change sets while the lease was held", second.count())
	}

	stop(t, cancelFirst, doneFirst)
	log.append(envelope(4))
	waitFor(t, "the takeover", func() bool { return second.count() >= 1 })
	stop(t, cancelSecond, doneSecond)
}

func TestChangeFeedPublishErrorAdvances(t *testing.T) {
	log := &changeLog{}
	log.append(envelope(1), envelope(2), envelope(3), envelope(4))
	pub := newPublisher()
	pub.errAt[3] = errors.New("bus down")
	checkpoints := newMemCheckpoints(time.Now())
	var mu sync.Mutex
	var failures []error
	cfg := baseConfig(log, newMemLeases(), checkpoints, pub.publish)
	cfg.OnError = func(err error) {
		mu.Lock()
		defer mu.Unlock()
		failures = append(failures, err)
	}
	cancel, done := runChangeFeed(t, cfg)

	waitFor(t, "the events after the failed one", func() bool { return pub.count() >= 3 })
	waitCheckpoint(t, checkpoints, 4)
	stop(t, cancel, done)

	if got, err := checkpoints.Load(context.Background(), "s1", "iam.changes", "live"); err != nil || got != 4 {
		t.Fatalf("checkpoint = %d, %v; want 4: a failed publish must not wedge the feed", got, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(failures) != 1 {
		t.Fatalf("OnError calls = %d, want 1", len(failures))
	}
}

func TestChangeFeedReadsAgainAfterAFullPage(t *testing.T) {
	log := &changeLog{}
	log.append(envelope(1), envelope(2), envelope(3), envelope(4))
	waiter := newWaiter()
	cfg := baseConfig(log, newMemLeases(), newMemCheckpoints(time.Now()), newPublisher().publish)
	cfg.Batch = 2
	cfg.Waiter = waiter
	cfg.Poll = time.Hour
	cancel, done := runChangeFeed(t, cfg)

	waitFor(t, "a wait after the last event", func() bool { return len(waiter.timeoutsSnapshot()) >= 1 })
	stop(t, cancel, done)

	reads := log.readsSnapshot()
	if len(reads) < 3 || reads[0] != 0 || reads[1] != 2 || reads[2] != 4 {
		t.Fatalf("reads = %v, want 0, 2, 4 (a full page is followed immediately)", reads)
	}
	for _, timeout := range waiter.timeoutsSnapshot() {
		if timeout != time.Hour {
			t.Fatalf("waiter timeout = %v, want the configured Poll %v", timeout, time.Hour)
		}
	}
}

func TestChangeFeedWakesOnTheWaiter(t *testing.T) {
	log := &changeLog{}
	log.append(envelope(1))
	waiter := newWaiter()
	pub := newPublisher()
	cfg := baseConfig(log, newMemLeases(), newMemCheckpoints(time.Now()), pub.publish)
	cfg.Waiter = waiter
	cfg.Poll = time.Hour
	cancel, done := runChangeFeed(t, cfg)
	waitFor(t, "the first event", func() bool { return pub.count() >= 1 })
	waitFor(t, "the waiter to park", func() bool { return len(waiter.timeoutsSnapshot()) >= 1 })

	log.append(envelope(3))
	waiter.signal()
	waitFor(t, "the woken event", func() bool { return pub.count() >= 2 })
	stop(t, cancel, done)
}

func TestChangeFeedStopsAndReleasesTheLease(t *testing.T) {
	log := &changeLog{}
	log.append(envelope(1))
	leases := newMemLeases()
	cancel, done := runChangeFeed(t, baseConfig(log, leases, newMemCheckpoints(time.Now()), newPublisher().publish))
	waitFor(t, "the feed to start", func() bool { return log.readCount() >= 1 })
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("change feed did not stop")
	}
	if leases.releaseCount() != 1 {
		t.Fatalf("releases = %d, want 1", leases.releaseCount())
	}
}

func TestChangeFeedStopsOnALostLease(t *testing.T) {
	log := &changeLog{}
	log.append(envelope(1))
	leases := newMemLeases()
	cfg := baseConfig(log, leases, newMemCheckpoints(time.Now()), newPublisher().publish)
	cfg.RenewEvery = 10 * time.Millisecond
	cfg.Poll = time.Hour
	cancel, done := runChangeFeed(t, cfg)
	waitFor(t, "the first event", func() bool { return log.readCount() >= 1 })
	n := leases.failRenewalsFromNow()
	waitFor(t, "the failed renewal", func() bool { return leases.renewCount() >= n })

	select {
	case err := <-done:
		if !errors.Is(err, feed.ErrLeaseLost) {
			t.Fatalf("Run = %v, want ErrLeaseLost", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the feed kept running after losing its lease")
	}
	cancel()
	if leases.releaseCount() != 1 {
		t.Fatalf("releases = %d, want 1", leases.releaseCount())
	}
}

func TestChangeFeedAdvancesPastAFinalEmptyKeyEvent(t *testing.T) {
	log := &changeLog{}
	log.append(envelope(1), envelope(2))
	checkpoints := newMemCheckpoints(time.Now())
	pub := newPublisher()
	cancel, done := runChangeFeed(t, baseConfig(log, newMemLeases(), checkpoints, pub.publish))

	// Event 2 has no keys and is last in the page: a feed that only
	// advanced its cursor on a publish would re-read it forever.
	waitCheckpoint(t, checkpoints, 2)
	stop(t, cancel, done)
	if pub.count() != 1 {
		t.Fatalf("published %d change sets, want 1", pub.count())
	}
}

func TestChangeFeedUsesTheScopedWaiter(t *testing.T) {
	log := &changeLog{}
	log.append(envelope(1))
	waiter := &scopedWakingWaiter{delay: 5 * time.Millisecond}
	cfg := baseConfig(log, newMemLeases(), newMemCheckpoints(time.Now()), newPublisher().publish)
	cfg.Waiter = waiter
	cfg.Poll = time.Hour
	cancel, done := runChangeFeed(t, cfg)

	waitFor(t, "a scoped wait", func() bool { return len(waiter.scopesSnapshot()) >= 1 })
	stop(t, cancel, done)
	if waiter.waitCalls() != 0 {
		t.Fatalf("the unscoped Wait was called %d times, want 0", waiter.waitCalls())
	}
	for _, scope := range waiter.scopesSnapshot() {
		if scope != "s1" {
			t.Fatalf("WaitScope scope = %q, want s1", scope)
		}
	}
}

func TestChangeFeedReleasesALeaseTakenWithAnError(t *testing.T) {
	log := &changeLog{}
	leases := newMemLeases()
	leases.acquireErr = errors.New("store committed then failed")
	leases.acquireTrueOnErr = true
	cfg := baseConfig(log, leases, newMemCheckpoints(time.Now()), newPublisher().publish)

	cf, err := feed.NewChangeFeed(cfg)
	if err != nil {
		t.Fatalf("new feed: %v", err)
	}
	if err := cf.Run(context.Background()); err == nil {
		t.Fatal("Run = nil, want the acquire error")
	}
	if leases.releaseCount() != 1 {
		t.Fatalf("releases = %d, want 1: the lease taken with the error must not leak", leases.releaseCount())
	}
}

func TestChangeFeedReadsAgainAfterATruncatedPage(t *testing.T) {
	log := &changeLog{}
	log.append(envelope(1), envelope(2), envelope(3))
	log.truncateAfter = 1
	waiter := newWaiter()
	cfg := baseConfig(log, newMemLeases(), newMemCheckpoints(time.Now()), newPublisher().publish)
	cfg.Waiter = waiter
	cfg.Poll = time.Hour
	cancel, done := runChangeFeed(t, cfg)

	waitFor(t, "a wait after the last event", func() bool { return len(waiter.timeoutsSnapshot()) >= 1 })
	stop(t, cancel, done)
	reads := log.readsSnapshot()
	if len(reads) < 3 || reads[0] != 0 || reads[1] != 1 || reads[2] != 2 {
		t.Fatalf("reads = %v, want 0,1,2: a truncated page is followed immediately", reads)
	}
}
