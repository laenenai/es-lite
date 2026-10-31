package natsjs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/es-lite/es"
)

// ErrGap reports that a delivered event does not follow the last one seen
// for its stream: a version was skipped, or its Es-Prev-Hash does not link
// to the last Es-Hash (a relay reorder, or a message that is not the row
// the store wrote). The consumer naks the message so it is redelivered
// after the missing one (ADR 0029 §6; review finding ES-04).
var ErrGap = errors.New("natsjs: per-stream gap or reorder detected")

// GapDetector keeps the last (version, hash) seen per stream and checks
// each incoming envelope against it. Safe for concurrent use. Memory is one
// entry per stream seen; call Forget for streams a consumer is done with.
type GapDetector struct {
	mu   sync.Mutex
	last map[string]gapEntry
}

type gapEntry struct {
	version uint64
	hash    []byte
}

// NewGapDetector returns an empty detector.
func NewGapDetector() *GapDetector { return &GapDetector{last: map[string]gapEntry{}} }

// Check validates e against the stream's last seen event and, when it is
// the next one, records it. The first event seen for a stream is accepted
// as-is (the detector may start mid-stream). A redelivery (version <= last)
// is accepted without updating. Returns ErrGap on a skipped version or a
// hash-link mismatch; hashes are compared only when both sides carry one.
func (g *GapDetector) Check(e es.Envelope) error {
	key := e.Workspace + "/" + e.StreamID.Canonical()
	g.mu.Lock()
	defer g.mu.Unlock()
	last, ok := g.last[key]
	switch {
	case !ok:
	case e.Version <= last.version:
		return nil // redelivery or duplicate: idempotent handlers absorb it
	case e.Version != last.version+1:
		return fmt.Errorf("%w: stream %s v%d after v%d", ErrGap, e.StreamID, e.Version, last.version)
	case len(last.hash) > 0 && len(e.PrevHash) > 0 && !bytes.Equal(last.hash, e.PrevHash):
		return fmt.Errorf("%w: stream %s v%d prev_hash does not link to v%d", ErrGap, e.StreamID, e.Version, last.version)
	}
	g.last[key] = gapEntry{version: e.Version, hash: e.Hash}
	return nil
}

// Forget drops the state kept for one stream.
func (g *GapDetector) Forget(workspace string, sid es.StreamID) {
	g.mu.Lock()
	delete(g.last, workspace+"/"+sid.Canonical())
	g.mu.Unlock()
}

// ProjectionHandler processes one delivered event. Returning nil acks it;
// returning an error naks it for redelivery. Delivery is at-least-once, so
// handlers MUST be idempotent (ADR 0003) — e.g. track the last-applied
// global_position per read model (ADR 0005).
type ProjectionHandler func(ctx context.Context, e es.Envelope) error

// ConsumerConfig configures a durable projection consumer.
type ConsumerConfig struct {
	Stream  string // stream to bind to
	Durable string // durable name = the projection's identity (its checkpoint)

	// FilterSubject narrows what the projection sees, e.g. "evt.*.counter.>"
	// for one aggregate across workspaces, or "evt.ws_a.>" for one workspace.
	// Empty consumes the whole stream.
	FilterSubject string

	// FilterSubjects is the multi-filter form (NATS server 2.10+): one
	// subject per consumed event type, e.g. the SubjectFor of every
	// `consumes` entry of a projection spec (ADR 0030 §2). Mutually exclusive
	// with FilterSubject.
	FilterSubjects []string

	// Gaps, when set, checks every delivered envelope for a per-stream gap
	// or reorder before the handler runs and naks on ErrGap (ADR 0029 §6).
	Gaps *GapDetector
}

// Consume runs a durable consumer until ctx is cancelled, decoding each
// message to an es.Envelope and invoking handler. To rebuild a projection,
// reset/recreate the durable and replay (ADR 0005).
func Consume(ctx context.Context, js jetstream.JetStream, cfg ConsumerConfig, handler ProjectionHandler) error {
	if cfg.FilterSubject != "" && len(cfg.FilterSubjects) > 0 {
		return fmt.Errorf("natsjs: consumer %q: FilterSubject and FilterSubjects are mutually exclusive", cfg.Durable)
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, cfg.Stream, jetstream.ConsumerConfig{
		Durable:        cfg.Durable,
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubject:  cfg.FilterSubject,
		FilterSubjects: cfg.FilterSubjects,
	})
	if err != nil {
		return fmt.Errorf("natsjs: create consumer %q: %w", cfg.Durable, err)
	}

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		e, err := DecodeEnvelope(msg.Headers(), msg.Data())
		if err != nil {
			_ = msg.Term() // undecodable: never redeliver
			return
		}
		if cfg.Gaps != nil {
			if err := cfg.Gaps.Check(e); err != nil {
				_ = msg.Nak() // redeliver after the missing message arrives
				return
			}
		}
		if err := handler(ctx, e); err != nil {
			_ = msg.Nak() // retry later
			return
		}
		_ = msg.Ack()
	})
	if err != nil {
		return fmt.Errorf("natsjs: consume %q: %w", cfg.Durable, err)
	}
	defer cc.Stop()

	<-ctx.Done()
	return ctx.Err()
}
