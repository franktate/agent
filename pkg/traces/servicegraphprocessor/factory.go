package servicegraphprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

const (
	// TypeStr is the unique identifier for the Prometheus service graph exporter.
	TypeStr = "service_graphs"

	// DefaultWait is the default value to wait for an edge to be completed
	DefaultWait = time.Second * 10
	// DefaultMaxItems is the default amount of edges that will be stored in the storeMap
	DefaultMaxItems = 10_000
	// DefaultWorkers is the default amount of workers that will be used to process the edges
	DefaultWorkers = 10
)

// Config holds the configuration for the Prometheus service graph processor.
type Config struct {
	component.Config `mapstructure:",squash"`

	Wait     time.Duration `mapstructure:"wait"`
	MaxItems int           `mapstructure:"max_items"`

	Workers int `mapstructure:"workers"`

	SuccessCodes *successCodes `mapstructure:"success_codes"`
}

type successCodes struct {
	http []int64 `mapstructure:"http"`
	grpc []int64 `mapstructure:"grpc"`
}

// NewFactory returns a new factory for the Prometheus service graph processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		TypeStr,
		createDefaultConfig,
		processor.WithTraces(createTracesProcessor, component.StabilityLevelUndefined),
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

func createTracesProcessor(
	_ context.Context,
	_ processor.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {

	eCfg := cfg.(*Config)
	return newProcessor(nextConsumer, eCfg), nil
}
