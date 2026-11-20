package postgres_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/feed"
	"github.com/laenenai/es-lite/postgres"
)

// TestChangeFeedReadsTheWorkspaceLog pins the real adapter: the change
// feed's ChangeLog is the unfiltered workspace store, its lease and
// checkpoint are the Postgres stores, and a second feed on the same key
// does not publish while the first holds the lease.
func TestChangeFeedReadsTheWorkspaceLog(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const scope = "ws_change_feed"
	ws := s.WorkspaceStore(scope)
	sid := streamID(t)

	first, err := ws.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          eventData("one"),
	})
	if err != nil {
		t.Fatalf("append one: %v", err)
	}
	second, err := ws.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: first.ToVersion,
		Events:          eventData("two"),
	})
	if err != nil {
		t.Fatalf("append two: %v", err)
	}
	want := []uint64{first.Envelopes[0].GlobalPosition, second.Envelopes[0].GlobalPosition}

	var (
		mu  sync.Mutex
		got []uint64
	)
	publish := func(_ context.Context, _ []string, position uint64) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, position)
		return nil
	}
	base := feed.ChangeFeedConfig{
		Scope:       scope,
		Name:        "iam.changes",
		Log:         ws,
		Leases:      postgres.NewLeaseStore(s),
		Checkpoints: postgres.NewCheckpointStore(s),
		Keys:        func(e es.Envelope) []string { return []string{e.StreamID.ID} },
		Publish:     publish,
		Poll:        10 * time.Millisecond,
		LeaseTTL:    time.Second,
		RenewEvery:  50 * time.Millisecond,
		Batch:       10,
	}

	firstCtx, cancelFirst := context.WithCancel(ctx)
	firstDone := make(chan error, 1)
	cf, err := feed.NewChangeFeed(base)
	if err != nil {
		t.Fatalf("new feed: %v", err)
	}
	go func() { firstDone <- cf.Run(firstCtx) }()
	t.Cleanup(cancelFirst)

	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the change feed did not publish both events")
		}
		time.Sleep(time.Millisecond)
	}

	// A second feed on the same lease key must stay silent.
	var secondMu sync.Mutex
	var secondGot int
	standby := base
	standby.Publish = func(context.Context, []string, uint64) error {
		secondMu.Lock()
		defer secondMu.Unlock()
		secondGot++
		return nil
	}
	cf2, err := feed.NewChangeFeed(standby)
	if err != nil {
		t.Fatalf("new standby: %v", err)
	}
	standbyCtx, cancelStandby := context.WithCancel(ctx)
	standbyDone := make(chan error, 1)
	go func() { standbyDone <- cf2.Run(standbyCtx) }()
	t.Cleanup(cancelStandby)
	time.Sleep(100 * time.Millisecond)
	secondMu.Lock()
	leaked := secondGot
	secondMu.Unlock()
	if leaked != 0 {
		t.Fatalf("the standby published %d change sets while the first held the lease", leaked)
	}

	cancelStandby()
	cancelFirst()
	select {
	case <-firstDone:
	case <-time.After(2 * time.Second):
		t.Fatal("the first feed did not stop")
	}
	select {
	case <-standbyDone:
	case <-time.After(2 * time.Second):
		t.Fatal("the standby did not stop")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("published positions = %v, want %v", got, want)
	}
}

// TestChangeFeedDoesNotSkipAnInFlightCommit is the end-to-end fence
// contract: tx2 commits its higher position while tx1's lower position is
// still open. The feed must publish nothing and must not advance its
// checkpoint, or the event that commits later is lost forever.
func TestChangeFeedDoesNotSkipAnInFlightCommit(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const scope = "ws_change_feed_ooc"
	ws := s.WorkspaceStore(scope)

	tx1, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	defer tx1.Rollback(ctx)
	gpA := feedAppendTx(t, ctx, ws, tx1, feedStream(t, "changes-ooc-a"), 0, "changes.v1.E1")

	tx2, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	defer tx2.Rollback(ctx)
	gpB := feedAppendTx(t, ctx, ws, tx2, feedStream(t, "changes-ooc-b"), 0, "changes.v1.E2")
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("commit tx2: %v", err)
	}

	var (
		mu  sync.Mutex
		got []uint64
	)
	cf, err := feed.NewChangeFeed(feed.ChangeFeedConfig{
		Scope:       scope,
		Name:        "iam.changes",
		Log:         ws,
		Leases:      postgres.NewLeaseStore(s),
		Checkpoints: postgres.NewCheckpointStore(s),
		Keys:        func(e es.Envelope) []string { return []string{e.StreamID.ID} },
		Publish: func(_ context.Context, _ []string, position uint64) error {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, position)
			return nil
		},
		Poll:       10 * time.Millisecond,
		LeaseTTL:   time.Second,
		RenewEvery: 50 * time.Millisecond,
		Batch:      10,
	})
	if err != nil {
		t.Fatalf("new feed: %v", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- cf.Run(runCtx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("the change feed did not stop")
		}
	})

	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	before := len(got)
	mu.Unlock()
	if before != 0 {
		t.Fatalf("the feed published %d change sets while gp=%d was in flight, want none", before, gpA)
	}

	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("commit tx1: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the feed did not publish both events after the lower commit")
		}
		time.Sleep(time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if got[0] != gpA || got[1] != gpB {
		t.Fatalf("published positions = %v, want [%d %d]", got, gpA, gpB)
	}
}
