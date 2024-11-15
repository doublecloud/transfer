package config

import (
	"github.com/doublecloud/transfer/pkg/transformer"
	"time"

	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/model"
)

type Endpoint struct {
	ID, Name string
	Type     abstract.ProviderType
	Params   string
}

type Runtime struct {
	Type   string
	Params interface{}
}

type UploadTables struct {
	Tables []abstract.TableDescription
}

type TransferYamlView struct {
	ID                string
	TransferName      string
	Description       string
	Labels            string
	Status            model.TransferStatus
	Type              abstract.TransferType
	FolderID          string
	CloudID           string
	CreatedAt         time.Time `db:"created_at"`
	Runtime           Runtime
	Src               Endpoint
	Dst               Endpoint
	RegularSnapshot   *abstract.RegularSnapshot `yaml:"regular_snapshot"`
	Transformation    transformer.Transformers  `yaml:"transformation"`
	DataObjects       *model.DataObjects        `yaml:"data_objects"`
	TypeSystemVersion int                       `yaml:"type_system_version"`
}
