package upload

import (
	"context"

	"github.com/doublecloud/transfer/cmd/trcli/config"
	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/worker/tasks"
	"github.com/spf13/cobra"
)

func UploadCommand() *cobra.Command {
	var transferParams string
	var uploadParams string

	uploadCommand := &cobra.Command{
		Use:     "upload",
		Short:   "Upload tables",
		Example: "./trcli upload --transfer ./transfer.yaml --tables tables.yaml",
		RunE:    upload(&transferParams, &uploadParams),
	}
	uploadCommand.Flags().StringVar(&transferParams, "transfer", "./transfer.yaml", "path to yaml file with transfer configuration")
	uploadCommand.Flags().StringVar(&uploadParams, "tables", "./tables.yaml", "path to yaml file with uploadable table params")

	return uploadCommand
}

func upload(transferYaml, uploadTablesYaml *string) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		transfer, err := config.TransferFromYaml(transferYaml)
		if err != nil {
			return xerrors.Errorf("unable to load transfer: %w", err)
		}

		tables, err := config.TablesFromYaml(uploadTablesYaml)
		if err != nil {
			return xerrors.Errorf("unable to load tables: %w", err)
		}

		return RunUpload(transfer, tables)
	}
}

func RunUpload(transfer *model.Transfer, tables *config.UploadTables) error {
	return tasks.Upload(
		context.Background(),
		coordinator.NewFakeClient(),
		*transfer,
		nil,
		tasks.UploadSpec{Tables: tables.Tables},
		solomon.NewRegistry(solomon.NewRegistryOpts()),
	)
}
