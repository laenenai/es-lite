// Package leader is a lightweight NATS-native leader election over a JetStream
// KV lease. Candidates race to Create a leader key in a TTL'd bucket; the
// winner refreshes it (Update with revision CAS) within the TTL; if it dies,
// the key expires and a follower takes over. While an instance holds the lease
// it runs the supplied work with a context cancelled the moment leadership is
// lost.
//
// It is intentionally lease-based, not perfectly fenced — a brief two-leader
// overlap during failover is acceptable for idempotent work (the es-lite relay
// dedups on global_position and claims rows with SKIP LOCKED, so overlap is
// harmless). Do NOT use it to fence work that requires exactly-one.
package leader

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Config configures an election.
type Config struct {
	Bucket string        // KV bucket (default "es_leader")
	Key    string        // leadership key (unique per elected role, e.g. "relay.<prefix>")
	ID     string        // this instance's unique id (e.g. host-pid)
	TTL    time.Duration // lease max-age; default 15s (failover ≈ TTL + poll)
}

// Run campaigns for leadership until ctx is cancelled. Each time this instance
// becomes leader it calls onLeader with a leaderCtx that is cancelled when
// leadership is lost (or ctx ends); onLeader should block until that ctx is
// done. Followers idle and retry. Returns ctx.Err() on shutdown.
func Run(ctx context.Context, js jetstream.JetStream, cfg Config, onLeader func(leaderCtx context.Context)) error {
	if cfg.Key == "" || cfg.ID == "" {
		return fmt.Errorf("leader: Key and ID are required")
	}
	if cfg.Bucket == "" {
		cfg.Bucket = "es_leader"
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 15 * time.Second
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: cfg.Bucket, TTL: cfg.TTL})
	if err != nil {
		return fmt.Errorf("leader: bucket %q: %w", cfg.Bucket, err)
	}
	refresh := cfg.TTL / 3
	if refresh < time.Second {
		refresh = time.Second
	}

	for ctx.Err() == nil {
		rev, acquired, err := acquire(ctx, kv, cfg)
		switch {
		case err != nil:
			// transient (e.g. NATS blip): back off and retry.
			if sleep(ctx, refresh) != nil {
				return ctx.Err()
			}
		case !acquired:
			// follower: wait out ~half a lease, then try to take over.
			if sleep(ctx, cfg.TTL/2) != nil {
				return ctx.Err()
			}
		default:
			// leader: run the work, holding+refreshing the lease until lost.
			leaderCtx, cancel := context.WithCancel(ctx)
			done := make(chan struct{})
			go func() { defer close(done); onLeader(leaderCtx) }()
			hold(ctx, kv, cfg, rev, refresh)
			cancel()
			<-done
		}
	}
	return ctx.Err()
}

func acquire(ctx context.Context, kv jetstream.KeyValue, cfg Config) (uint64, bool, error) {
	rev, err := kv.Create(ctx, cfg.Key, []byte(cfg.ID))
	if err == nil {
		return rev, true, nil
	}
	if errors.Is(err, jetstream.ErrKeyExists) {
		return 0, false, nil // held by another instance (or not yet expired)
	}
	return 0, false, err
}

// hold refreshes the lease every `refresh` until an Update fails (leadership
// lost: revision moved, or the key expired and was recreated) or ctx ends.
func hold(ctx context.Context, kv jetstream.KeyValue, cfg Config, rev uint64, refresh time.Duration) {
	t := time.NewTicker(refresh)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			next, err := kv.Update(ctx, cfg.Key, []byte(cfg.ID), rev)
			if err != nil {
				return // lost leadership
			}
			rev = next
		}
	}
}

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
