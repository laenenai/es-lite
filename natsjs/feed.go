package natsjs

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/projection"
)

// NewFeed adapts a durable JetStream consumer to projection.Feed, so a
// generated projector or reaction runner (ADR 0030) runs over JetStream
// exactly as it runs over projection.LogFeed in tests. Typical wiring:
//
//	feed := natsjs.NewFeed(js, natsjs.ConsumerConfig{
//		Stream:         "ES_EVENTS",
//		Durable:        counterviewv1.CounterViewName,
//		FilterSubjects: counterviewv1.CounterViewSubjects("*"),
//		Gaps:           natsjs.NewGapDetector(),
//	})
//	err := runner.Run(ctx, feed)
func NewFeed(js jetstream.JetStream, cfg ConsumerConfig) projection.Feed {
	return projection.FeedFunc(func(ctx context.Context, handle func(context.Context, es.Envelope) error) error {
		return Consume(ctx, js, cfg, handle)
	})
}
