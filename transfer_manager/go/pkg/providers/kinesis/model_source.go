package kinesis

import (
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
)

var (
	_ model.Source = (*KinesisSource)(nil)
)

type KinesisSource struct {
	Endpoint     string
	Region       string
	Stream       string
	BufferSize   int
	AccessKey    string
	SecretKey    model.SecretString
	ParserConfig map[string]interface{}
}

func (k *KinesisSource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (k *KinesisSource) Validate() error {
	return nil
}

func (k *KinesisSource) WithDefaults() {
	if k.BufferSize == 0 {
		k.BufferSize = 128 * 1024 * 1024
	}
}

func (k *KinesisSource) IsSource() {}
