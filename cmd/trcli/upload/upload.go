package upload

import (
	"context"

	"github.com/doublecloud/transfer/cmd/trcli/config"
	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/worker/tasks"
	"github.com/spf13/cobra"
)

func UploadCommand(cp *coordinator.Coordinator, rt abstract.Runtime) *cobra.Command {
	var transferParams string
	var uploadParams string

	uploadCommand := &cobra.Command{
		Use:     "upload",
		Short:   "Upload tables",
		Example: "./trcli upload --transfer ./transfer.yaml --tables tables.yaml",
		RunE:    upload(cp, rt, &transferParams, &uploadParams),
	}
	uploadCommand.Flags().StringVar(&transferParams, "transfer", "./transfer.yaml", "path to yaml file with transfer configuration")
	uploadCommand.Flags().StringVar(&uploadParams, "tables", "./tables.yaml", "path to yaml file with uploadable table params")

	return uploadCommand
}

func upload(cp *coordinator.Coordinator, rt abstract.Runtime, transferYaml, uploadTablesYaml *string) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		transfer, err := config.TransferFromYaml(transferYaml)
		if err != nil {
			return xerrors.Errorf("unable to load transfer: %w", err)
		}

		transfer.Runtime = rt

		tables, err := config.TablesFromYaml(uploadTablesYaml)
		if err != nil {
			return xerrors.Errorf("unable to load tables: %w", err)
		}

		return RunUpload(*cp, transfer, tables)
	}
}

func RunUpload(cp coordinator.Coordinator, transfer *model.Transfer, tables *config.UploadTables) error {
	return tasks.Upload(
		context.Background(),
		cp,
		*transfer,
		nil,
		tasks.UploadSpec{Tables: tables.Tables},
		solomon.NewRegistry(solomon.NewRegistryOpts()),
	)
}
