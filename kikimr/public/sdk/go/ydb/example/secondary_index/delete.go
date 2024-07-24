package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/table"
)

func doDelete(
	ctx context.Context,
	sp *table.SessionPool,
	prefix string,
	args ...string,
) error {
	if len(args) == 0 {
		return fmt.Errorf("id of series arguemnt required")
	}
	s, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return err
	}

	c, err := deleteTransaction(ctx, sp, prefix, s)
	if err != nil {
		return err
	}
	fmt.Printf("Deleted %v rows", c)
	return nil
}

func deleteTransaction(ctx context.Context, sp *table.SessionPool, prefix string, seriesID uint64,
) (count uint64, err error) {
	query := fmt.Sprintf(`
        PRAGMA TablePathPrefix("%v");

        DECLARE $seriesId AS Uint64;

        -- Simulate a DESC index by inverting views using max(uint64)-views
        $maxUint64 = 0xffffffffffffffff;

        $data = (
            SELECT series_id, ($maxUint64 - views) AS rev_views
            FROM `+"`series`"+`
            WHERE series_id = $seriesId
        );

        DELETE FROM series
        ON SELECT series_id FROM $data;

        DELETE FROM series_rev_views
        ON SELECT rev_views, series_id FROM $data;

        SELECT COUNT(*) AS cnt FROM $data;`, prefix)

	writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())

	var res *table.Result
	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			stmt, err := s.Prepare(ctx, query)
			if err != nil {
				return err
			}
			_, res, err = stmt.Execute(ctx, writeTx,
				table.NewQueryParameters(
					table.ValueParam("$seriesId", ydb.Uint64Value(seriesID)),
				))
			return err
		}))
	if err != nil {
		return
	}
	if res.NextSet() && res.NextRow() && res.NextItem() {
		count = res.Uint64()
	}
	return
}
