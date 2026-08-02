package natsstore

import (
	"context"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/laenenai/natskit"
)

// ObservabilityMiddleware returns a natskit middleware that records an OTel span
// and request metrics (count + duration, labelled by method and outcome) per
// request, propagating incoming W3C trace context from the message headers so
// traces span the async hop. It uses the global providers installed by
// obs.Setup — inert when OTLP is not configured. Wire it via
// natsstore.WithMiddleware(ObservabilityMiddleware("es-lited")).
func ObservabilityMiddleware(service string) natskit.Middleware {
	tracer := otel.Tracer(service)
	meter := otel.Meter(service)
	reqs, _ := meter.Int64Counter("eslite.requests", metric.WithDescription("es-lited request count"))
	dur, _ := meter.Float64Histogram("eslite.request.duration",
		metric.WithUnit("s"), metric.WithDescription("es-lited request duration"))
	prop := otel.GetTextMapPropagator()

	return func(next natskit.MsgHandler) natskit.MsgHandler {
		return func(ctx context.Context, m natskit.MsgContext) error {
			ctx = prop.Extract(ctx, propagation.HeaderCarrier(m.Header))
			method := lastToken(m.Subject)
			ctx, span := tracer.Start(ctx, method,
				trace.WithSpanKind(trace.SpanKindServer),
				trace.WithAttributes(attribute.String("subject", m.Subject)))

			start := time.Now()
			err := next(ctx, m)

			outcome := "ok"
			if err != nil {
				outcome = "error"
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			attrs := metric.WithAttributes(
				attribute.String("method", method),
				attribute.String("outcome", outcome))
			reqs.Add(ctx, 1, attrs)
			dur.Record(ctx, time.Since(start).Seconds(), attrs)
			span.End()
			return err
		}
	}
}

func lastToken(subject string) string {
	if i := strings.LastIndexByte(subject, '.'); i >= 0 {
		return subject[i+1:]
	}
	return subject
}
