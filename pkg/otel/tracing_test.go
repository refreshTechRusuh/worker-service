package otel

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

func captureOTelSetters(t *testing.T) (*trace.TracerProvider, *propagation.TextMapPropagator) {
	t.Helper()
	var provider trace.TracerProvider
	var propagator propagation.TextMapPropagator
	previousSetProvider, previousSetPropagator := setTracerProvider, setTextMapPropagator
	setTracerProvider = func(value trace.TracerProvider) { provider = value }
	setTextMapPropagator = func(value propagation.TextMapPropagator) { propagator = value }
	t.Cleanup(func() {
		setTracerProvider, setTextMapPropagator = previousSetProvider, previousSetPropagator
	})
	return &provider, &propagator
}

func TestSetupTracerProviderDisabled(t *testing.T) {
	provider, propagator := captureOTelSetters(t)
	t.Setenv("OTEL_ENABLED", "false")

	shutdown, err := SetupTracerProvider(context.Background(), "worker-test", "test-version")
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	assert.NoError(t, shutdown(context.Background()))

	ctx, span := (*provider).Tracer("worker-test").Start(context.Background(), "noop-span")
	assert.NotNil(t, ctx)
	span.End()
	assert.NotNil(t, *propagator)
}

func TestSetupTracerProviderExportsSpansAndUsesResource(t *testing.T) {
	provider, _ := captureOTelSetters(t)
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_SAMPLE_RATIO", "1")

	previousExporter, previousDetector := createTraceExporter, detectResource
	t.Cleanup(func() {
		createTraceExporter = previousExporter
		detectResource = previousDetector
	})
	exporter := tracetest.NewInMemoryExporter()
	createTraceExporter = func() (sdktrace.SpanExporter, error) { return exporter, nil }
	var detected []string
	detectResource = func(_ context.Context, service, version string) (*sdkresource.Resource, error) {
		detected = []string{service, version}
		return sdkresource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceName(service), semconv.ServiceVersion(version),
			attribute.String("test.resource", "injected")), nil
	}

	shutdown, err := SetupTracerProvider(context.Background(), "worker-test", "1.2.3")
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	ctx, span := (*provider).Tracer("worker-test").Start(context.Background(), "test-span")
	assert.NotNil(t, ctx)
	assert.True(t, span.IsRecording())
	span.End()
	tracerProvider := (*provider).(*sdktrace.TracerProvider)
	require.NoError(t, tracerProvider.ForceFlush(context.Background()))

	assert.Equal(t, []string{"worker-test", "1.2.3"}, detected)
	require.Len(t, exporter.GetSpans(), 1)
	assert.Equal(t, "test-span", exporter.GetSpans()[0].Name)
	require.NoError(t, shutdown(context.Background()))
}

func TestSetupTracerProviderUsesFallbackResource(t *testing.T) {
	provider, _ := captureOTelSetters(t)
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_SAMPLE_RATIO", "1")

	previousExporter, previousDetector := createTraceExporter, detectResource
	t.Cleanup(func() {
		createTraceExporter = previousExporter
		detectResource = previousDetector
	})
	exporter := tracetest.NewInMemoryExporter()
	createTraceExporter = func() (sdktrace.SpanExporter, error) { return exporter, nil }
	detectResource = func(context.Context, string, string) (*sdkresource.Resource, error) {
		return nil, errors.New("metadata server unavailable")
	}

	shutdown, err := SetupTracerProvider(context.Background(), "worker-fallback", "2.0.0")
	require.NoError(t, err)
	_, span := (*provider).Tracer("worker-test").Start(context.Background(), "fallback-span")
	span.End()
	tracerProvider := (*provider).(*sdktrace.TracerProvider)
	require.NoError(t, tracerProvider.ForceFlush(context.Background()))
	require.Len(t, exporter.GetSpans(), 1)
	assert.Equal(t, "fallback-span", exporter.GetSpans()[0].Name)
	require.NoError(t, shutdown(context.Background()))
}

func TestSetupTracerProviderExporterFailure(t *testing.T) {
	_, propagator := captureOTelSetters(t)
	t.Setenv("OTEL_ENABLED", "true")
	previousExporter := createTraceExporter
	t.Cleanup(func() { createTraceExporter = previousExporter })
	wantErr := errors.New("exporter unavailable")
	createTraceExporter = func() (sdktrace.SpanExporter, error) { return nil, wantErr }

	shutdown, err := SetupTracerProvider(context.Background(), "worker-test", "1.0.0")
	assert.Nil(t, shutdown)
	assert.ErrorIs(t, err, wantErr)
	assert.Nil(t, *propagator)
}

func TestSampleRatioBoundsAndInvalidInput(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  float64
	}{
		{name: "default", value: "", want: 1},
		{name: "valid", value: "0.25", want: 0.25},
		{name: "invalid", value: "not-a-number", want: 1},
		{name: "below zero", value: "-0.5", want: 0},
		{name: "above one", value: "1.5", want: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OTEL_SAMPLE_RATIO", tc.value)
			assert.Equal(t, tc.want, sampleRatio())
		})
	}
}

func TestSetupSetsCompositePropagator(t *testing.T) {
	provider, propagator := captureOTelSetters(t)
	t.Setenv("OTEL_ENABLED", "false")
	_, err := SetupTracerProvider(context.Background(), "worker-test", "1.0.0")
	require.NoError(t, err)
	require.NotNil(t, *provider)
	expected := propagation.NewCompositeTextMapPropagator()
	assert.IsType(t, expected, *propagator)
	ctx, span := Tracer("worker-test").Start(context.Background(), "global-wrapper")
	assert.NotNil(t, ctx)
	span.End()
}
