package replicate

import (
	"github.com/doublecloud/tross/library/go/core/metrics/solomon"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/cmd/trcli/config"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/coordinator"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/dataplane/provideradapter"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/runtime/local"
	"github.com/spf13/cobra"
)

func ReplicateCommand() *cobra.Command {
	var transferParams string
	replicationCommand := &cobra.Command{
		Use:   "replicate",
		Short: "Start local replication",
		RunE:  replicate(&transferParams),
	}
	replicationCommand.Flags().StringVar(&transferParams, "transfer", "./transfer.yaml", "path to yaml file with transfer configuration")
	return replicationCommand
}

func replicate(transferYaml *string) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		transfer, err := config.TransferFromYaml(transferYaml)
		if err != nil {
			return xerrors.Errorf("unable to load transfer: %w", err)
		}
		return RunReplication(transfer)
	}
}

func RunReplication(transfer *model.Transfer) error {
	if err := provideradapter.ApplyForTransfer(transfer); err != nil {
		return xerrors.Errorf("unable to adapt transfer: %w", err)
	}
	for {
		worker := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.Log)
		err := worker.Run()
		if abstract.IsFatal(err) {
			return err
		}
		if err := worker.Stop(); err != nil {
			logger.Log.Warnf("unable to stop worker: %v", err)
		}
		logger.Log.Warnf("worker failed: %v, restart", err)
	}
}
