package activate

import (
	"context"

	"github.com/doublecloud/tross/library/go/core/metrics/solomon"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/cmd/trcli/config"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/coordinator"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/worker/tasks"
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
	return tasks.ActivateDelivery(
		context.Background(),
		nil,
		coordinator.NewFakeClient(),
		*transfer,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
	)
}
