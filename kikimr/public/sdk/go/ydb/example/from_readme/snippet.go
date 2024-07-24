package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/connect"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/example/internal/cli"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/table"
)

type Command struct {
}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
	connectCtx, cancel := context.WithTimeout(ctx, params.ConnectTimeout)
	defer cancel()
	db, err := connect.New(connectCtx, params.ConnectParams)
	if err != nil {
		return fmt.Errorf("connect error: %w", err)
	}
	defer db.Close()

	err = prepareScheme(ctx, db, params.Prefix())
	if err != nil {
		return fmt.Errorf("error on prepare scheme: %w", err)
	}

	err = prepareData(ctx, db, params.Prefix())
	if err != nil {
		return fmt.Errorf("error on prepare data: %w", err)
	}

	s, err := db.Table().CreateSession(ctx)
	if err != nil {
		return fmt.Errorf("error on create session: %w", err)
	}

	defer func() {
		_ = s.Close(ctx)
	}()

	// Prepare transaction control for upcoming query execution.
	// NOTE: result of TxControl() may be reused.
	txc := table.TxControl(
		table.BeginTx(table.WithSerializableReadWrite()),
		table.CommitTx(),
	)
	// Execute text query without preparation and with given "autocommit"
	// transaction control. That is, transaction will be commited without
	// additional calls. Notice the "_" unused variable – it stands for created
	// transaction during execution, but as said above, transaction is commited
	// for us and we do not want to do anything with it.
	_, res, err := s.Execute(ctx, txc,
		`
			DECLARE $mystr AS Utf8?;

			SELECT 42 as id, $mystr as mystr;
		`,
		table.NewQueryParameters(
			table.ValueParam("$mystr", ydb.OptionalValue(ydb.UTF8Value("test"))),
		),
	)
	if err != nil {
		return err // handle error
	}
	// Scan for received values within the result set(s).
	// res.Err() reports the reason of last unsuccessful one.
	var (
		id    int32
		myStr *string //optional value
	)
	for res.NextResultSet(ctx, "id", "mystr") {
		for res.NextRow() {
			// Suppose our "users" table has two rows: id and age.
			// Thus, current row will contain two appropriate items with
			// exactly the same order.
			err := res.Scan(&id, &myStr)

			// Error handling.
			if err != nil {
				return err
			}
			// do something with data
			fmt.Printf("got id %v, got mystr: %v\n", id, *myStr)
		}
	}
	if res.Err() != nil {
		return res.Err() // handle error
	}

	return nil
}

func (cmd *Command) ExportFlags(context.Context, *flag.FlagSet) {}
