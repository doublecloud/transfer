package check

import (
	"context"

	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/cmd/trcli/config"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/worker/tasks"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

func CheckCommand() *cobra.Command {
	var transferParams string
	checkCommand := &cobra.Command{
		Use:   "check",
		Short: "Check transfer locally",
		Args:  cobra.MatchAll(cobra.ExactArgs(0)),
		RunE:  check(&transferParams),
	}
	checkCommand.Flags().StringVar(&transferParams, "transfer", "./transfer.yaml", "path to yaml file with transfer configuration")
	return checkCommand
}

func check(transferYaml *string) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		transfer, err := config.TransferFromYaml(transferYaml)
		if err != nil {
			return xerrors.Errorf("unable to load transfer: %w", err)
		}
		return RunCheck(transfer)
	}
}

func testSource(transfer model.Transfer) error {
	transformationJSON, err := transfer.TransformationJSON()
	if err != nil {
		return xerrors.Errorf("unable to serialize transformation: %w", err)
	}
	res := tasks.TestEndpoint(context.Background(), &tasks.TestEndpointParams{
		Transfer:             &transfer,
		Type:                 transfer.SrcType(),
		Params:               transfer.SrcJSON(),
		IsSource:             true,
		TransformationConfig: transformationJSON,
	}, abstract.NewTestResult())

	prettyRes, err := yaml.Marshal(res)
	if err != nil {
		return err
	}

	logger.Log.Infof("check source done: found %d objects:\n\n%s", len(res.Schema), string(prettyRes))
	return nil
}

func testTarget(transfer model.Transfer) error {
	if res := tasks.TestEndpoint(context.Background(), &tasks.TestEndpointParams{
		Transfer:             &transfer,
		Type:                 transfer.DstType(),
		Params:               transfer.DstJSON(),
		IsSource:             false,
		TransformationConfig: nil,
	}, abstract.NewTestResult()); res.Err() != nil {
		return xerrors.Errorf("unable to check target: %w", res.Err())
	}
	logger.Log.Infof("check target done")
	return nil
}

func RunCheck(transfer *model.Transfer) error {
	if err := testSource(*transfer); err != nil {
		return xerrors.Errorf("failed testing source: %w", err)
	}

	if err := testTarget(*transfer); err != nil {
		return xerrors.Errorf("failed testing target: %w", err)
	}
	return nil
}
