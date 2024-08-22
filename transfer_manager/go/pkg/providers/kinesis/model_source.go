package kinesis

import (
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers"
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

func (s *KinesisSource) IsAppendOnly() bool {
	if s.ParserConfig == nil {
		return true
	} else {
		parserConfigStruct, _ := parsers.ParserConfigMapToStruct(s.ParserConfig)
		if parserConfigStruct == nil {
			return true
		}
		return parserConfigStruct.IsAppendOnly()
	}
}
