package natsjs

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/es-lite/delivery"
	"github.com/laenenai/es-lite/es"
)

// Publisher.Handle is a delivery.Handler: it plugs into the SQLite
// delivery.Poller and (with the same signature) the Postgres Store.Drain.
var _ delivery.Handler = (&Publisher{}).Handle

// Connect opens a NATS connection. Callers may instead bring their own
// *nats.Conn (e.g. from natskit.Connect) and pass it to jetstream.New.
func Connect(url string, opts ...nats.Option) (*nats.Conn, error) {
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, fmt.Errorf("natsjs: connect: %w", err)
	}
	return nc, nil
}

// StreamConfig configures the JetStream stream that captures es-lite events.
type StreamConfig struct {
	Name     string   // e.g. "ES_EVENTS"
	Subjects []string // default ["evt.>"]

	// Duplicates is the dedup window keyed on Nats-Msg-Id (= global_position).
	// It MUST exceed max relay lag + failover time, so a re-electing relay's
	// replays fall inside it (ADR 0003). Default 2m.
	Duplicates time.Duration

	// MaxAge bounds retention. Retention is a fast-replay convenience, not an
	// archive — deep rebuilds read the DB log. 0 keeps per server limits.
	MaxAge time.Duration
}

// EnsureStream creates or updates the events stream.
func EnsureStream(ctx context.Context, js jetstream.JetStream, cfg StreamConfig) (jetstream.Stream, error) {
	if len(cfg.Subjects) == 0 {
		cfg.Subjects = []string{"evt.>"}
	}
	if cfg.Duplicates == 0 {
		cfg.Duplicates = 2 * time.Minute
	}
	return js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       cfg.Name,
		Subjects:   cfg.Subjects,
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy,
		Duplicates: cfg.Duplicates,
		MaxAge:     cfg.MaxAge,
	})
}

// Publisher publishes envelopes to JetStream. Its Handle method is a
// delivery.Handler, so it drops straight into the Postgres Store.Drain or the
// SQLite delivery.Poller as the relay's publish step.
type Publisher struct {
	js      jetstream.JetStream
	subject SubjectFunc
}

// NewPublisher builds a Publisher. subject may be nil (uses DefaultSubject).
func NewPublisher(js jetstream.JetStream, subject SubjectFunc) *Publisher {
	if subject == nil {
		subject = DefaultSubject
	}
	return &Publisher{js: js, subject: subject}
}

// Publish sends one envelope, deduplicated on global_position.
func (p *Publisher) Publish(ctx context.Context, e es.Envelope) error {
	if _, err := p.js.PublishMsg(ctx, toMsg(p.subject(e), e)); err != nil {
		return fmt.Errorf("natsjs: publish gp=%d: %w", e.GlobalPosition, err)
	}
	return nil
}

// Handle publishes a batch in order, stopping at the first error so the
// caller (Drain / Poller) does not advance its checkpoint past an event that
// was not published. Signature matches delivery.Handler and Drain's publish.
func (p *Publisher) Handle(ctx context.Context, batch []es.Envelope) error {
	for _, e := range batch {
		if err := p.Publish(ctx, e); err != nil {
			return err
		}
	}
	return nil
}
