package stdout

import (
	"time"

	"github.com/doublecloud/transfer/pkg/abstract"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/middlewares/async/bufferer"
)

type StdoutDestination struct {
	ShowData          bool
	TransformerConfig map[string]string
	TriggingCount     int
	TriggingSize      uint64
	TriggingInterval  time.Duration
}

var _ server.Destination = (*StdoutDestination)(nil)

func (StdoutDestination) WithDefaults() {
}

func (d *StdoutDestination) Transformer() map[string]string {
	return d.TransformerConfig
}

func (d *StdoutDestination) CleanupMode() server.CleanupType {
	return server.DisabledCleanup
}

func (StdoutDestination) IsDestination() {
}

func (d *StdoutDestination) GetProviderType() abstract.ProviderType {
	return ProviderTypeStdout
}

func (d *StdoutDestination) Validate() error {
	return nil
}

func (d *StdoutDestination) BuffererConfig() bufferer.BuffererConfig {
	return bufferer.BuffererConfig{
		TriggingCount:    d.TriggingCount,
		TriggingSize:     d.TriggingSize,
		TriggingInterval: d.TriggingInterval,
	}
}
