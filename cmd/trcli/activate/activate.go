package activate

import (
	"context"
	"time"

	"github.com/doublecloud/transfer/cmd/trcli/config"
	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/worker/tasks"
	"github.com/spf13/cobra"
)

func ActivateCommand(cp *coordinator.Coordinator, rt abstract.Runtime, registry metrics.Registry) *cobra.Command {
	var transferParams string
	var activateDelay time.Duration
	activationCommand := &cobra.Command{
		Use:   "activate",
		Short: "Activate transfer locally",
		Args:  cobra.MatchAll(cobra.ExactArgs(0)),
		RunE:  activate(cp, rt, &transferParams, registry, activateDelay),
	}
	activationCommand.Flags().StringVar(&transferParams, "transfer", "./transfer.yaml", "path to yaml file with transfer configuration")
	activationCommand.Flags().DurationVar(&activateDelay, "min-delay", 10*time.Second, "minial delay for activation, use to ensure metrics got scrapped, default 10s")
	return activationCommand
}

func activate(
	cp *coordinator.Coordinator,
	rt abstract.Runtime,
	transferYaml *string,
	registry metrics.Registry,
	delay time.Duration,
) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		transfer, err := config.TransferFromYaml(transferYaml)
		if err != nil {
			return xerrors.Errorf("unable to load transfer: %w", err)
		}
		transfer.Runtime = rt
		return RunActivate(*cp, transfer, registry, delay)
	}
}

func RunActivate(
	cp coordinator.Coordinator,
	transfer *model.Transfer,
	registry metrics.Registry,
	delay time.Duration,
) error {
	st := time.Now()
	defer func() {
		if time.Since(st) < delay {
			extraWait := delay.Truncate(time.Since(st))
			logger.Log.Infof("activation done faster then minimal delay, wait for: %v", extraWait)
			time.Sleep(extraWait)
		}
	}()
	logger.Log.Infof("run activate with: %T", cp)
	op := new(model.TransferOperation)
	op.OperationID = transfer.ID + "/activation"
	err := tasks.ActivateDelivery(
		context.Background(),
		op,
		cp,
		*transfer,
		registry,
	)

	if err != nil {
		return xerrors.Errorf("activation failed with: %w", err)
	}

	pcp, ok := cp.(coordinator.Progressable)
	if !ok {
		logger.Log.Info("Activation completed")
		return nil
	}
	logger.Log.Infof("Activation completed, upload: %v parts", len(pcp.Progress()))
	for _, p := range pcp.Progress() {
		logger.Log.Infof("	part: %s 👌 %v rows in %v", p.String(), p.CompletedRows, time.Since(st))
	}
	return nil
}
