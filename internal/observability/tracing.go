package observability

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	tracerOnce     sync.Once
	tracerProvider *sdktrace.TracerProvider
	globalTracer   trace.Tracer
)

// InitTracing initializes OpenTelemetry tracing with OTLP exporter.
func InitTracing(ctx context.Context, nodeID, endpoint string) error {
	var initErr error
	tracerOnce.Do(func() {
		exporter, err := otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(endpoint),
			otlptracegrpc.WithInsecure(),
		)
		if err != nil {
			initErr = err
			return
		}

		res, err := resource.New(ctx,
			resource.WithAttributes(
				semconv.ServiceName("forgekv"),
				semconv.ServiceInstanceID(nodeID),
			),
		)
		if err != nil {
			initErr = err
			return
		}

		tracerProvider = sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exporter),
			sdktrace.WithResource(res),
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
		)

		otel.SetTracerProvider(tracerProvider)
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))

		globalTracer = tracerProvider.Tracer("forgekv")
	})
	return initErr
}

// Tracer returns the global tracer.
func Tracer() trace.Tracer {
	if globalTracer == nil {
		return otel.Tracer("forgekv")
	}
	return globalTracer
}

// ShutdownTracing shuts down the tracer provider.
func ShutdownTracing(ctx context.Context) error {
	if tracerProvider != nil {
		return tracerProvider.Shutdown(ctx)
	}
	return nil
}

// StartSpan starts a new span with the given name.
func StartSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	return Tracer().Start(ctx, name)
}

// SpanFromContext extracts the span from context.
func SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}

// TraceIDFromContext extracts the trace ID string from context.
func TraceIDFromContext(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if span == nil {
		return ""
	}
	sc := span.SpanContext()
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}

// AddEvent adds an event to the current span.
func AddEvent(ctx context.Context, name string) {
	span := trace.SpanFromContext(ctx)
	if span != nil {
		span.AddEvent(name)
	}
}

// SetError records an error on the current span.
func SetSpanError(ctx context.Context, err error) {
	span := trace.SpanFromContext(ctx)
	if span != nil && err != nil {
		span.RecordError(err)
	}
}
