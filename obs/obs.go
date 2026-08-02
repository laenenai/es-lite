// Package obs is es-lite's optional OpenTelemetry setup. It exports metrics and
// traces over OTLP to the mesh's OpenObserve collector. It is a no-op unless
// OTEL_EXPORTER_OTLP_ENDPOINT is set, so tests, SQLite, and single-node stay
// zero-config; production sets the standard OTEL_* env (endpoint, headers,
// insecure) and everything is configured from there.
//
// OpenObserve ingests OTLP directly; point OTEL_EXPORTER_OTLP_ENDPOINT at it
// (e.g. http://openobserve:5081) and pass its auth via OTEL_EXPORTER_OTLP_HEADERS.
package obs

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Enabled reports whether OTLP export is configured.
func Enabled() bool { return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" }

// Setup installs global OTLP metric + trace providers and the W3C trace-context
// propagator, returning a shutdown func. When OTLP is not configured it is a
// no-op (shutdown returns nil). All exporter options come from the standard
// OTEL_* environment variables.
func Setup(ctx context.Context, serviceName, version string) (func(context.Context) error, error) {
	if !Enabled() {
		return func(context.Context) error { return nil }, nil
	}

	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithProcess(),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(version),
		),
	)
	if err != nil {
		return nil, err
	}

	traceExp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	metricExp, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	return func(c context.Context) error {
		_ = tp.Shutdown(c)
		return mp.Shutdown(c)
	}, nil
}
