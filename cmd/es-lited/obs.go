package main

// Local, minimal stand-ins for natskit/obs (ADR 0028), which is a private
// dependency this public repo can't build against. Behavior is preserved:
// structured stdout logging always on, and OTLP metrics/traces entirely
// inert (the OTel SDK is never wired up, so otel.Meter/otel.Tracer calls
// fall back to the library's built-in no-ops) unless
// OTEL_EXPORTER_OTLP_ENDPOINT is set — matching the "no-op unless configured"
// behavior documented in docs/deploy/README.md.

import (
	"context"
	"log"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/laenenai/es-lite/natsstore"
)

// natsConnect opens a NATS connection with a connection name (visible in
// `nats-server`/`nats micro` tooling) and sensible reconnect behavior for a
// long-running service: reconnect indefinitely, log (dis)connects, and count
// reconnects as nats_reconnects_total — a local replacement for
// natskit.Connect. If creds is non-empty it is used as a NATS credentials
// file (nats.UserCredentials); TLS is whatever the nats.go defaults /
// NATS_URL scheme provide (natskit additionally read NATS_CA / client-cert
// env vars for uniform TLS — not replicated here, see report).
func natsConnect(name, url, creds string) (*nats.Conn, error) {
	reconnects, _ := otel.Meter(name).Int64Counter("nats_reconnects_total",
		metric.WithDescription("NATS client reconnects"))
	opts := []nats.Option{
		nats.Name(name),
		nats.MaxReconnects(-1), // reconnect forever; this is a long-running service
		nats.ReconnectWait(2 * time.Second),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("%s: reconnected to %s", name, nc.ConnectedUrl())
			reconnects.Add(context.Background(), 1)
		}),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				log.Printf("%s: nats disconnected: %v", name, err)
			}
		}),
		nats.ClosedHandler(func(*nats.Conn) {
			log.Printf("%s: nats connection closed", name)
		}),
	}
	if creds != "" {
		opts = append(opts, nats.UserCredentials(creds))
	}
	return nats.Connect(url, opts...)
}

// initLogging configures process-wide structured (JSON) logging via
// log/slog, tagging every line with the service name, and routes the
// standard "log" package (used throughout this command) through it.
func initLogging(service string) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", service)
	slog.SetDefault(logger)
	log.SetFlags(0)
	log.SetOutput(logWriter{logger})
}

type logWriter struct{ logger *slog.Logger }

func (w logWriter) Write(p []byte) (int, error) {
	w.logger.Info(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// setupObs wires OTLP metrics + traces to OTEL_EXPORTER_OTLP_ENDPOINT
// (OTEL_EXPORTER_OTLP_HEADERS and OTEL_SERVICE_NAME are honored automatically
// by the OTLP exporters / resource detection). It is a no-op — the global
// no-op MeterProvider/TracerProvider stay in place — when the endpoint is
// unset, so dev/single-node deployments remain zero-config.
func setupObs(ctx context.Context, service, version string) (func(context.Context) error, error) {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}

	name := service
	if v := os.Getenv("OTEL_SERVICE_NAME"); v != "" {
		name = v
	}
	res, err := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceName(name),
		semconv.ServiceVersion(version),
	))
	if err != nil {
		return nil, err
	}

	metricExp, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)),
	)
	otel.SetMeterProvider(mp)

	traceExp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExp),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return func(shutCtx context.Context) error {
		err1 := mp.Shutdown(shutCtx)
		if err2 := tp.Shutdown(shutCtx); err2 != nil && err1 == nil {
			err1 = err2
		}
		return err1
	}, nil
}

// natsHeaderCarrier adapts a natsstore.MsgContext's headers to OTel's
// TextMapCarrier, so a W3C trace context set by the caller propagates across
// the NATS hop into the server span (docs/deploy/README.md's "Traces").
type natsHeaderCarrier natsstore.MsgContext

func (c natsHeaderCarrier) Get(key string) string { return c.Header.Get(key) }
func (c natsHeaderCarrier) Set(key, value string) { c.Header.Set(key, value) }
func (c natsHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c.Header))
	for k := range c.Header {
		keys = append(keys, k)
	}
	return keys
}

// serverMiddleware records eslite.requests (count, labelled method+outcome)
// and eslite.request.duration (histogram, seconds), and opens one server
// span per request — mirroring the metrics/traces natskit/obs.ServerMiddleware
// provided (docs/deploy/README.md's "Observability").
func serverMiddleware(service string) natsstore.Middleware {
	meter := otel.Meter(service)
	requests, _ := meter.Int64Counter("eslite.requests",
		metric.WithDescription("es-lited requests, by method and outcome"))
	duration, _ := meter.Float64Histogram("eslite.request.duration",
		metric.WithDescription("es-lited request duration"), metric.WithUnit("s"))
	tracer := otel.Tracer(service)

	return func(next natsstore.SvcHandler) natsstore.SvcHandler {
		return func(ctx context.Context, m natsstore.MsgContext) ([]byte, error) {
			if m.Header != nil {
				ctx = otel.GetTextMapPropagator().Extract(ctx, natsHeaderCarrier(m))
			}
			method := natsstore.SubjectToken(m.Subject, strings.Count(m.Subject, "."))
			ctx, span := tracer.Start(ctx, method, trace.WithSpanKind(trace.SpanKindServer))
			start := time.Now()
			b, err := next(ctx, m)
			outcome := "ok"
			if err != nil {
				outcome = "error"
				span.RecordError(err)
			}
			span.End()
			attrs := metric.WithAttributes(
				attribute.String("method", method),
				attribute.String("outcome", outcome),
			)
			requests.Add(ctx, 1, attrs)
			duration.Record(ctx, time.Since(start).Seconds(), attrs)
			return b, err
		}
	}
}
