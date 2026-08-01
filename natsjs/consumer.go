package natsjs

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/es-lite/es"
)

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
}

// Consume runs a durable consumer until ctx is cancelled, decoding each
// message to an es.Envelope and invoking handler. To rebuild a projection,
// reset/recreate the durable and replay (ADR 0005).
func Consume(ctx context.Context, js jetstream.JetStream, cfg ConsumerConfig, handler ProjectionHandler) error {
	cons, err := js.CreateOrUpdateConsumer(ctx, cfg.Stream, jetstream.ConsumerConfig{
		Durable:       cfg.Durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: cfg.FilterSubject,
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
