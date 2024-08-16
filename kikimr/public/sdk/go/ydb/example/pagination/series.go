package main

import (
	"context"
	"flag"
	"fmt"
	"path"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/connect"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/example/internal/cli"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/table"
)

type Command struct {
}

func (cmd *Command) ExportFlags(context.Context, *flag.FlagSet) {}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
	connectCtx, cancel := context.WithTimeout(ctx, params.ConnectTimeout)
	defer cancel()
	db, err := connect.New(connectCtx, params.ConnectParams)
	if err != nil {
		return fmt.Errorf("connect error: %w", err)
	}
	defer db.Close()

	err = db.CleanupDatabase(ctx, params.Prefix(), "schools")
	if err != nil {
		return err
	}
	err = db.EnsurePathExists(ctx, params.Prefix())
	if err != nil {
		return err
	}

	err = createTable(ctx, db.Table().Pool(), path.Join(params.Prefix(), "schools"))
	if err != nil {
		return fmt.Errorf("create tables error: %w", err)
	}

	err = fillTableWithData(ctx, db.Table().Pool(), params.Prefix())
	if err != nil {
		return fmt.Errorf("fill tables with data error: %w", err)
	}

	lastNum := 0
	lastCity := ""
	limit := 3
	maxPages := 10
	for i, empty := 0, false; i < maxPages && !empty; i++ {
		fmt.Printf("> Page %v:\n", i+1)
		empty, err = selectPaging(ctx, db.Table().Pool(), params.Prefix(), limit, &lastNum, &lastCity)
		if err != nil {
			return fmt.Errorf("get page %v error: %w", i, err)
		}
	}

	return nil
}

func selectPaging(
	ctx context.Context, sp *table.SessionPool, prefix string, limit int, lastNum *int, lastCity *string) (
	empty bool, err error) {

	var query = fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $limit AS Uint64;
		DECLARE $lastCity AS Utf8;
		DECLARE $lastNumber AS Uint32;

		$Data = (
			SELECT * FROM schools
			WHERE city = $lastCity AND number > $lastNumber

			UNION ALL

			SELECT * FROM schools
			WHERE city > $lastCity
			ORDER BY city, number LIMIT $limit
		);
		SELECT * FROM $Data ORDER BY city, number LIMIT $limit;`, prefix)

	readTx := table.TxControl(table.BeginTx(table.WithOnlineReadOnly()), table.CommitTx())

	var res *table.Result
	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			_, res, err = s.Execute(ctx, readTx, query,
				table.NewQueryParameters(
					table.ValueParam("$limit", ydb.Uint64Value(uint64(limit))),
					table.ValueParam("$lastCity", ydb.UTF8Value(*lastCity)),
					table.ValueParam("$lastNumber", ydb.Uint32Value(uint32(*lastNum))),
				),
			)
			return
		}),
	)
	if err != nil {
		return
	}
	if err = res.Err(); err != nil {
		return
	}
	if !res.NextSet() || !res.HasNextRow() {
		empty = true
		return
	}

	for res.NextRow() {
		res.SeekItem("city")
		*lastCity = res.OUTF8()

		res.SeekItem("number")
		*lastNum = int(res.OUint32())

		res.SeekItem("address")
		addr := res.OUTF8()

		fmt.Printf("\t%v, School #%v, Address: %v\n", *lastCity, *lastNum, addr)
	}
	return
}

func fillTableWithData(ctx context.Context, sp *table.SessionPool, prefix string) (err error) {
	var query = fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $schoolsData AS List<Struct<
			city: Utf8,
			number: Uint32,
			address: Utf8>>;

		REPLACE INTO schools
		SELECT
			city,
			number,
			address
		FROM AS_TABLE($schoolsData);`, prefix)

	writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())

	return table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			_, _, err = s.Execute(ctx, writeTx, query, table.NewQueryParameters(
				table.ValueParam("$schoolsData", getSchoolData()),
			))
			return err
		}),
	)
}

func createTable(ctx context.Context, sp *table.SessionPool, path string) (err error) {
	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			return s.CreateTable(ctx, path,
				table.WithColumn("city", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("number", ydb.Optional(ydb.TypeUint32)),
				table.WithColumn("address", ydb.Optional(ydb.TypeUTF8)),
				table.WithPrimaryKeyColumn("city", "number"),
			)
		}),
	)
	if err != nil {
		return err
	}

	return nil
}
