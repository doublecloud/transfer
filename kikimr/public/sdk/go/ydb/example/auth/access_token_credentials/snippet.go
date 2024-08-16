package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/connect"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/example/internal/cli"
)

type Command struct {
	accessToken string
}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
	connectCtx, cancel := context.WithTimeout(ctx, params.ConnectTimeout)
	defer cancel()
	db, err := connect.New(
		connectCtx,
		params.ConnectParams,
		connect.WithAccessTokenCredentials(cmd.accessToken),
	)
	if err != nil {
		return fmt.Errorf("connect error: %w", err)
	}
	defer db.Close()

	// work with db instance

	return nil
}

func (cmd *Command) ExportFlags(_ context.Context, flagSet *flag.FlagSet) {
	flagSet.StringVar(&cmd.accessToken, "ydb-access-token", "", "access token for YDB authenticate")
}
