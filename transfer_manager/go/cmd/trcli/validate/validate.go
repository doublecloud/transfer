package validate

import (
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/cmd/trcli/config"
	"github.com/spf13/cobra"
)

func ValidateCommand() *cobra.Command {
	var transferParams string
	validationCommand := &cobra.Command{
		Use:   "validate",
		Short: "Validate a transfer configuration",
		RunE:  validate(&transferParams),
	}
	validationCommand.Flags().StringVar(&transferParams, "transfer", "./transfer.yaml", "path to yaml file with transfer configuration")
	return validationCommand
}

func validate(transferYaml *string) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		transfer, err := config.TransferFromYaml(transferYaml)
		if err != nil {
			return xerrors.Errorf("unable to load transfer: %w", err)
		}

		if err := transfer.Src.Validate(); err != nil {
			return xerrors.Errorf("source validation failed: %w", err)
		}

		if err := transfer.Dst.Validate(); err != nil {
			return xerrors.Errorf("target validation failed: %w", err)
		}

		if err := transfer.Validate(); err != nil {
			return xerrors.Errorf("transfer validation failed: %w", err)
		}

		return nil
	}
}
