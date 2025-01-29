package upload

import (
	"context"

	"github.com/doublecloud/transfer/cmd/trcli/config"
	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/worker/tasks"
	"github.com/spf13/cobra"
)

func UploadCommand(cp *coordinator.Coordinator, rt abstract.Runtime, registry metrics.Registry) *cobra.Command {
	var transferParams string
	var uploadParams string

	uploadCommand := &cobra.Command{
		Use:     "upload",
		Short:   "Upload tables",
		Example: "./trcli upload --transfer ./transfer.yaml --tables tables.yaml",
		RunE:    upload(cp, rt, &transferParams, &uploadParams, registry),
	}
	uploadCommand.Flags().StringVar(&transferParams, "transfer", "./transfer.yaml", "path to yaml file with transfer configuration")
	uploadCommand.Flags().StringVar(&uploadParams, "tables", "./tables.yaml", "path to yaml file with uploadable table params")

	return uploadCommand
}

func upload(cp *coordinator.Coordinator, rt abstract.Runtime, transferYaml, uploadTablesYaml *string, registry metrics.Registry) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		transfer, err := config.TransferFromYaml(transferYaml)
		if err != nil {
			return xerrors.Errorf("unable to load transfer: %w", err)
		}

		transfer.Runtime = rt

		if transfer.Telemetry != nil && transfer.Telemetry.Prefix != "" {
			registry = registry.WithPrefix(transfer.Telemetry.Prefix)
		}

		tables, err := config.TablesFromYaml(uploadTablesYaml)
		if err != nil {
			return xerrors.Errorf("unable to load tables: %w", err)
		}

		return RunUpload(*cp, transfer, tables, registry)
	}
}

func RunUpload(cp coordinator.Coordinator, transfer *model.Transfer, tables *config.UploadTables, registry metrics.Registry) error {
	return tasks.Upload(
		context.Background(),
		cp,
		*transfer,
		nil,
		tasks.UploadSpec{Tables: tables.Tables},
		registry.WithTags(map[string]string{
			"resource_id": transfer.ID,
			"name":        transfer.TransferName,
		}),
	)
}
