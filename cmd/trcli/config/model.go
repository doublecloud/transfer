package config

import (
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/transformer"
	"gopkg.in/yaml.v2"
)

type Endpoint struct {
	Type   abstract.ProviderType
	Params any
}

func (e Endpoint) RawParams() string {
	switch p := e.Params.(type) {
	case []byte:
		return string(p)
	case string:
		return p
	default:
		data, _ := yaml.Marshal(p)
		return string(data)
	}
}

type Runtime struct {
	Type   string      `json:"type" yaml:"type"`
	Params interface{} `json:"params" yaml:"params"`
}

type UploadTables struct {
	Tables []abstract.TableDescription `json:"tables" yaml:"tables"`
}

type TransferYamlView struct {
	ID                string                    `json:"id" yaml:"id"`
	TransferName      string                    `json:"transfer_name" yaml:"transfer_name"`
	Description       string                    `json:"description" yaml:"description"`
	Labels            string                    `json:"labels" yaml:"labels"`
	Status            model.TransferStatus      `json:"status" yaml:"status"`
	Type              abstract.TransferType     `json:"type" yaml:"type"`
	FolderID          string                    `json:"folder_id" yaml:"folder_id"`
	CloudID           string                    `json:"cloud_id" yaml:"cloud_id"`
	Src               Endpoint                  `json:"src" yaml:"src"`
	Dst               Endpoint                  `json:"dst" yaml:"dst"`
	RegularSnapshot   *abstract.RegularSnapshot `json:"regular_snapshot" yaml:"regular_snapshot"`
	Transformation    *transformer.Transformers `json:"transformation" yaml:"transformation"`
	DataObjects       *model.DataObjects        `json:"data_objects" yaml:"data_objects"`
	TypeSystemVersion int                       `json:"type_system_version" yaml:"type_system_version"`
}
