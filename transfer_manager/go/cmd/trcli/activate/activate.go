package activate

import (
	"context"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/cmd/trcli/config"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/worker/tasks"
	"github.com/spf13/cobra"
)

func ActivateCommand() *cobra.Command {
	var transferParams string
	activationCommand := &cobra.Command{
		Use:   "activate",
		Short: "Activate transfer locally",
		Args:  cobra.MatchAll(cobra.ExactArgs(0)),
		RunE:  activate(&transferParams),
	}
	activationCommand.Flags().StringVar(&transferParams, "transfer", "./transfer.yaml", "path to yaml file with transfer configuration")
	return activationCommand
}

func activate(transferYaml *string) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		transfer, err := config.TransferFromYaml(transferYaml)
		if err != nil {
			return xerrors.Errorf("unable to load transfer: %w", err)
		}
		return RunActivate(transfer)
	}
}

func RunActivate(transfer *model.Transfer) error {
	cp := coordinator.NewStatefulFakeClient()
	st := time.Now()
	err := tasks.ActivateDelivery(
		context.Background(),
		nil,
		cp,
		*transfer,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
	)

	if err != nil {
		return xerrors.Errorf("activation failed with: %w", err)
	}

	logger.Log.Infof("Activation completed, upload: %v parts", len(cp.Progres()))
	for _, p := range cp.Progres() {
		logger.Log.Infof("	part: %s 👌 %v rows in %v", p.String(), p.CompletedRows, time.Since(st))
	}
	return nil
}
