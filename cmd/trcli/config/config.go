package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"gopkg.in/yaml.v2"
	sig_yaml "sigs.k8s.io/yaml"
)

func TransferFromYaml(params *string) (*model.Transfer, error) {
	dir, err := os.MkdirTemp("", "shared-volume")
	if err != nil {
		return nil, xerrors.Errorf("unable to create base dir: %w", err)
	}
	_ = os.Setenv("BASE_DIR", dir)

	transferRaw, err := os.ReadFile(*params)
	if err != nil {
		return nil, xerrors.Errorf("unable to read yaml config file: %w", err)
	}

	transfer, err := ParseTransfer(transferRaw)
	if err != nil {
		return nil, xerrors.Errorf("unable to parse yaml config to transfer: %w", err)
	}
	return transfer, nil
}

func ParseTransfer(yaml []byte) (*model.Transfer, error) {
	tr, err := ParseTransferYaml(yaml)
	if err != nil {
		return nil, xerrors.Errorf("unable to parse yaml: %w", err)
	}

	source, err := source(tr)
	if err != nil {
		return nil, xerrors.Errorf("failed to construct source: %w", err)
	}

	target, err := target(tr)
	if err != nil {
		return nil, xerrors.Errorf("failed to construct target: %w", err)
	}

	transfer := transfer(source, target, tr)

	transfer.FillDependentFields()
	if len(tr.Transformation.Transformers) > 0 {
		transfer.Transformation = &model.Transformation{
			Transformers:      &tr.Transformation,
			ExtraTransformers: nil,
			Executor:          nil,
			RuntimeJobIndex:   0,
		}
	}
	return transfer, nil
}

func ParseTransferYaml(rawData []byte) (*TransferYamlView, error) {
	var transfer TransferYamlView
	if err := yaml.Unmarshal(rawData, &transfer); err != nil {
		return nil, err
	}
	for _, v := range os.Environ() {
		pair := strings.SplitN(v, "=", 2)
		transfer.Src.Params = strings.ReplaceAll(transfer.Src.Params, fmt.Sprintf("${%v}", pair[0]), pair[1])
		transfer.Dst.Params = strings.ReplaceAll(transfer.Dst.Params, fmt.Sprintf("${%v}", pair[0]), pair[1])
	}
	res, err := sig_yaml.YAMLToJSON([]byte(transfer.Src.Params))
	if err == nil {
		transfer.Src.Params = string(res)
	}
	res, err = sig_yaml.YAMLToJSON([]byte(transfer.Dst.Params))
	if err == nil {
		transfer.Dst.Params = string(res)
	}
	return &transfer, nil
}

func TablesFromYaml(tablesParams *string) (*UploadTables, error) {
	rawData, err := os.ReadFile(*tablesParams)
	if err != nil {
		return nil, xerrors.Errorf("unable to read tables config file: %w", err)
	}
	tables, err := ParseTablesYaml(rawData)
	if err != nil {
		return nil, xerrors.Errorf("unable to parse tables config file: %w", err)
	}
	return tables, nil
}

func ParseTablesYaml(rawData []byte) (*UploadTables, error) {
	var tables UploadTables
	if err := yaml.Unmarshal(rawData, &tables); err != nil {
		return nil, err
	}
	return &tables, nil
}

func source(tr *TransferYamlView) (model.Source, error) {
	return model.NewSource(tr.Src.Type, tr.Src.Params)
}

func target(tr *TransferYamlView) (model.Destination, error) {
	return model.NewDestination(tr.Dst.Type, tr.Dst.Params)
}

func transfer(source model.Source, target model.Destination, tr *TransferYamlView) *model.Transfer {
	rt := new(abstract.LocalRuntime)
	rt.WithDefaults()
	transfer := new(model.Transfer)
	transfer.ID = tr.ID
	transfer.TransferName = tr.TransferName
	transfer.Description = tr.Description
	transfer.Labels = tr.Labels
	transfer.Status = model.New
	transfer.Type = tr.Type
	transfer.FolderID = tr.FolderID
	transfer.Runtime = rt
	transfer.Src = source
	transfer.Dst = target
	transfer.CloudID = tr.CloudID
	transfer.RegularSnapshot = tr.RegularSnapshot
	transfer.DataObjects = tr.DataObjects
	transfer.TypeSystemVersion = tr.TypeSystemVersion
	return transfer
}
