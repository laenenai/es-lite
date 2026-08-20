// Package natskv implements snapshot.Cache over a NATS KV bucket. It is
// workspace-scoped and zero-knowledge (ADR 0025): it stores the snapshot state
// bytes it is given AS-IS, exactly as the event store treats payloads. Any
// sealing is the caller's job — pre-seal snap.State with your client codec and
// KV holds only ciphertext. Give the bucket a TTL to bound staleness and
// auto-evict; a snapshot is a pure cache, so expiry is always safe.
package natskv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/es-lite/snapshot"
)

// Config configures a Cache.
type Config struct {
	Bucket string        // KV bucket (default "es_snapshots")
	TTL    time.Duration // per-entry max age; 0 = no expiry
}

// Cache is a workspace-scoped snapshot.Cache backed by NATS KV.
type Cache struct {
	kv jetstream.KeyValue
	ws string
}

var _ snapshot.Cache = (*Cache)(nil)

// New creates/opens the bucket and returns a Cache scoped to workspace.
func New(ctx context.Context, js jetstream.JetStream, workspace string, cfg Config) (*Cache, error) {
	if cfg.Bucket == "" {
		cfg.Bucket = "es_snapshots"
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: cfg.Bucket,
		TTL:    cfg.TTL,
	})
	if err != nil {
		return nil, fmt.Errorf("natskv: bucket %q: %w", cfg.Bucket, err)
	}
	return &Cache{kv: kv, ws: workspace}, nil
}

// entry is the KV-stored form: metadata + opaque state bytes ([]byte is base64
// in JSON).
type entry struct {
	Version     uint64 `json:"version"`
	FoldVersion uint32 `json:"fold_version"`
	RecordedAt  string `json:"recorded_at"`
	State       []byte `json:"state"`
}

// key namespaces snapshots by workspace; KV keys allow [-/_=.a-zA-Z0-9], so
// other characters (e.g. the ':' in a canonical stream id) are sanitized.
func (c *Cache) key(streamID string) string {
	return sanitize(c.ws) + "." + sanitize(streamID)
}

// Load implements snapshot.Cache.
func (c *Cache) Load(ctx context.Context, streamID string) (snapshot.Snapshot, bool, error) {
	kve, err := c.kv.Get(ctx, c.key(streamID))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return snapshot.Snapshot{}, false, nil
	}
	if err != nil {
		return snapshot.Snapshot{}, false, err
	}
	var e entry
	if err := json.Unmarshal(kve.Value(), &e); err != nil {
		return snapshot.Snapshot{}, false, err
	}
	rec, _ := time.Parse(time.RFC3339Nano, e.RecordedAt)
	return snapshot.Snapshot{Version: e.Version, FoldVersion: e.FoldVersion, RecordedAt: rec, State: e.State}, true, nil
}

// Save implements snapshot.Cache. The state is stored opaquely (ADR 0025).
func (c *Cache) Save(ctx context.Context, streamID string, snap snapshot.Snapshot) error {
	b, err := json.Marshal(entry{
		Version:     snap.Version,
		FoldVersion: snap.FoldVersion,
		RecordedAt:  snap.RecordedAt.UTC().Format(time.RFC3339Nano),
		State:       snap.State,
	})
	if err != nil {
		return err
	}
	_, err = c.kv.Put(ctx, c.key(streamID), b)
	return err
}

func sanitize(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}
