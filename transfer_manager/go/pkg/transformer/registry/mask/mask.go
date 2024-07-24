package mask

import (
	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/transformer"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/transformer/registry/filter"
)

func init() {
	transformer.Register[Config](
		MaskFieldTransformerType,
		func(protoConfig Config, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
			return NewMaskTransformer(protoConfig, lgr)
		},
	)
}

type Config struct {
	MaskFunctionHash MaskFunctionHash `json:"maskFunctionHash"`
	Tables           filter.Tables    `json:"tables"`
	Columns          []string         `json:"columns"`
}

type MaskFunctionHash struct {
	UserDefinedSalt string `json:"userDefinedSalt"`
}

func NewMaskTransformer(config Config, lgr log.Logger) (abstract.Transformer, error) {
	tables, err := filter.NewFilter(config.Tables.IncludeTables, config.Tables.ExcludeTables)
	if err != nil {
		return nil, xerrors.Errorf("unable to init tables filter: %w", err)
	}
	columns := config.Columns

	hashingTransformer, err := NewHmacHasherTransformer(config.MaskFunctionHash, lgr, tables, columns)
	if err != nil {
		return nil, xerrors.Errorf("cannot make hash transformer: %w", err)
	}
	return hashingTransformer, nil
}
