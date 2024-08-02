package stdout

import (
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/config"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/metering/writer"
)

func init() {
	config.RegisterTypeTagged((*config.WriterConfig)(nil), (*Stdout)(nil), (*Stdout)(nil).Type(), nil)
	writer.Register(
		new(Stdout).Type(),
		func(schema, topic, sourceID string, cfg writer.WriterConfig) (writer.Writer, error) {
			return &Writer{}, nil
		},
	)
}

type Stdout struct{}

func (*Stdout) IsMeteringWriter() {}
func (*Stdout) IsTypeTagged()     {}
func (*Stdout) Type() string      { return "stdout" }

type Writer struct {
}

func (Writer) Close() error {
	return nil
}

func (Writer) Write(data string) error {
	logger.Log.Infof("metering:\n%s", data)
	return nil
}
