package main

// Local, minimal stand-ins for natskit/obs (ADR 0028), which is a private
// dependency this public repo can't build against. Behavior is preserved:
// structured stdout logging always on, and OTLP metrics/traces entirely
// inert (the OTel SDK is never wired up, so otel.Meter calls fall back to
// the library's built-in no-op) unless OTEL_EXPORTER_OTLP_ENDPOINT is set —
// matching the "no-op unless configured" behavior documented in
// docs/deploy/README.md.

import (
	"context"
	"log"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
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
// unset, so dev/single-node deployments remain zero-config. es-relayd's own
// eslite.relayed counter (main.go) is read from the global MeterProvider this
// installs.
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
