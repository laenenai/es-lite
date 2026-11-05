package feed

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The stubs exist only to satisfy NewServer's dependency checks; no method is
// called by the tests in this file.
type stubCheckpoints struct{}

func (stubCheckpoints) Load(context.Context, string, string, string) (uint64, error) {
	return 0, nil
}
func (stubCheckpoints) Save(context.Context, string, string, string, uint64) error { return nil }
func (stubCheckpoints) Reset(context.Context, string, string, string, uint64) error {
	return nil
}
func (stubCheckpoints) Delete(context.Context, string, string, string) error { return nil }
func (stubCheckpoints) Checkpoint(context.Context, string, string, string) (Checkpoint, bool, error) {
	return Checkpoint{}, false, nil
}

type stubLeases struct{}

func (stubLeases) Acquire(context.Context, string, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (stubLeases) Renew(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (stubLeases) Release(context.Context, string, string) error { return nil }
func (stubLeases) Holder(context.Context, string) (Lease, bool, error) {
	return Lease{}, false, nil
}

type stubLogs struct{}

func (stubLogs) Open(context.Context, string) (Log, error) { return nil, nil }

func TestNewServerNormalizesRenewEvery(t *testing.T) {
	base := Config{Checkpoints: stubCheckpoints{}, Leases: stubLeases{}, Logs: stubLogs{}}
	cases := []struct {
		name       string
		ttl, renew time.Duration
		want       time.Duration
	}{
		{"defaults leave renew below ttl", 0, 0, defaultRenewEvery},
		{"equal is reduced to a third", 30 * time.Second, 30 * time.Second, 10 * time.Second},
		{"greater is reduced to a third", 9 * time.Second, 20 * time.Second, 3 * time.Second},
		{"valid explicit interval is kept", 30 * time.Second, 5 * time.Second, 5 * time.Second},
		{"sub-nanosecond third falls back to a half", 2 * time.Nanosecond, 2 * time.Nanosecond, time.Nanosecond},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := base
			cfg.LeaseTTL, cfg.RenewEvery = c.ttl, c.renew
			srv, err := NewServer(cfg)
			if err != nil {
				t.Fatalf("NewServer: %v", err)
			}
			if srv.cfg.RenewEvery != c.want {
				t.Fatalf("RenewEvery = %v, want %v", srv.cfg.RenewEvery, c.want)
			}
			if srv.cfg.RenewEvery >= srv.cfg.LeaseTTL {
				t.Fatalf("RenewEvery = %v is not below LeaseTTL = %v", srv.cfg.RenewEvery, srv.cfg.LeaseTTL)
			}
		})
	}
}

func TestNewServerRejectsAnUnrenewableLeaseTTL(t *testing.T) {
	cfg := Config{
		Checkpoints: stubCheckpoints{}, Leases: stubLeases{}, Logs: stubLogs{},
		LeaseTTL: time.Nanosecond, RenewEvery: time.Nanosecond,
	}
	if _, err := NewServer(cfg); err == nil {
		t.Fatal("NewServer accepted a 1ns lease TTL it cannot renew")
	}
}

// Without entropy no safe owner can be minted: a constant prefix would
// collide across processes. NewServer must propagate the failure, not mask it.
func TestNewPrefixPropagatesEntropyError(t *testing.T) {
	cause := errors.New("entropy source failed")
	orig := randRead
	randRead = func([]byte) (int, error) { return 0, cause }
	t.Cleanup(func() { randRead = orig })

	_, err := NewServer(Config{Checkpoints: stubCheckpoints{}, Leases: stubLeases{}, Logs: stubLogs{}})
	if err == nil {
		t.Fatal("NewServer minted a prefix without entropy; want an error")
	}
	if !errors.Is(err, cause) {
		t.Fatalf("error = %v, want the entropy cause", err)
	}
}

// Negative MaxPayloadBytes is kept as-is (the stream treats it as "check
// disabled"); negative MaxBatchBytes is replaced by the default, because the
// Log contract requires a positive budget.
func TestNewServerNegativeConfigSemantics(t *testing.T) {
	base := Config{Checkpoints: stubCheckpoints{}, Leases: stubLeases{}, Logs: stubLogs{}}

	cfg := base
	cfg.MaxPayloadBytes = -1
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if srv.cfg.MaxPayloadBytes != -1 {
		t.Fatalf("MaxPayloadBytes = %d, want -1 kept (negative disables the check)", srv.cfg.MaxPayloadBytes)
	}

	cfg = base
	cfg.MaxBatchBytes = -1
	srv, err = NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if srv.cfg.MaxBatchBytes != defaultMaxBatchBytes {
		t.Fatalf("MaxBatchBytes = %d, want the default %d", srv.cfg.MaxBatchBytes, defaultMaxBatchBytes)
	}
}
