package tracing

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

var (
	serviceName = os.Getenv("OTEL_SERVICE_NAME")
)

// InitTracer initializes the OpenTelemetry tracer
func InitTracer(ctx context.Context) (func(context.Context) error, error) {
	if serviceName == "" {
		serviceName = "accelerated-container-image"
	}

	// Create resource with service information
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating resource: %w", err)
	}

	var exporters []sdktrace.SpanExporter

	// Create OTLP exporter (default)
	client := otlptracegrpc.NewClient()
	otlpExporter, err := otlptrace.New(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("creating OTLP trace exporter: %w", err)
	}
	exporters = append(exporters, otlpExporter)

	// Add console exporter if enabled
	//if os.Getenv("OTEL_TRACES_CONSOLE") == "true" {
	consoleExporter, err := stdouttrace.New(
		stdouttrace.WithPrettyPrint(),
	)
	if err != nil {
		return nil, fmt.Errorf("creating console trace exporter: %w", err)
	}
	exporters = append(exporters, consoleExporter)
	//}

	// Create trace provider options
	var opts []sdktrace.TracerProviderOption
	opts = append(opts, sdktrace.WithResource(res))
	opts = append(opts, sdktrace.WithSampler(sdktrace.AlwaysSample()))

	// Add batch processors for each exporter
	for _, exp := range exporters {
		opts = append(opts, sdktrace.WithBatcher(exp))
	}

	tp := sdktrace.NewTracerProvider(opts...)

	// Set global trace provider
	otel.SetTracerProvider(tp)

	// Set up trace propagation (for distributed tracing across services)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Return shutdown function
	return tp.Shutdown, nil
}
