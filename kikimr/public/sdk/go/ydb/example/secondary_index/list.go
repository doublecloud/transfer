package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"text/tabwriter"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/table"
)

func doList(
	ctx context.Context,
	sp *table.SessionPool,
	prefix string,
	args ...string,
) error {
	arg, err := parsListArgs(args)
	if err != nil {
		return fmt.Errorf(`wrong arguments. Usage: limit 1 last-id 2
			limit - Maximum number of rows
			last-id - Resume from this last series id
			Error: %v`, err)
	}

	var res *table.Result
	if lastID, id := arg["last-id"]; id {
		res, err = listByIDSeries(ctx, sp, prefix, arg["limit"], lastID)
	} else {
		res, err = listByID(ctx, sp, prefix, arg["limit"])
	}
	if err != nil {
		return err
	}
	if res.NextSet() {
		series := SeriesList{}
		err = series.Scan(res)
		if err != nil {
			return err
		}
		return printSeries(series)
	}
	return nil
}

func doListViews(
	ctx context.Context,
	sp *table.SessionPool,
	prefix string,
	args ...string,
) error {
	arg, err := parsListArgs(args)
	if err != nil {
		return fmt.Errorf(`wrong arguments. Usage: limit 1 last-id 2 last-views 3
			limit - Maximum number of rows
			last-id - Resume from this last series id
			last-views - Resume from this last series views
			Error: %v`, err)
	}

	var res *table.Result
	lastID, id := arg["last-id"]
	lastView, view := arg["last-views"]
	if id && view {
		res, err = listByViewsSeries(ctx, sp, prefix, arg["limit"], lastID, lastView)
		if err != nil {
			return err
		}
	} else {
		res, err = listByViews(ctx, sp, prefix, arg["limit"])
		if err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	if res.NextSet() {
		series := SeriesList{}
		err = series.Scan(res)
		if err != nil {
			return err
		}
		return printSeries(series)
	}
	return nil
}

func listByID(ctx context.Context, sp *table.SessionPool, prefix string, limit uint64) (res *table.Result, err error) {
	query := fmt.Sprintf(`
        PRAGMA TablePathPrefix("%v");

        DECLARE $limit AS Uint64;

        SELECT series_id, title, series_info, release_date, views
        FROM series
        ORDER BY series_id
        LIMIT $limit;`, prefix)

	writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())

	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			stmt, err := s.Prepare(ctx, query)
			if err != nil {
				return err
			}
			_, res, err = stmt.Execute(ctx, writeTx,
				table.NewQueryParameters(
					table.ValueParam("$limit", ydb.Uint64Value(limit)),
				))
			return err
		}))
	return
}

func listByIDSeries(ctx context.Context, sp *table.SessionPool, prefix string, limit, lastSeries uint64,
) (res *table.Result, err error) {
	query := fmt.Sprintf(`
        PRAGMA TablePathPrefix("%v");

        DECLARE $limit AS Uint64;
        DECLARE $lastSeriesId AS Uint64;

        SELECT series_id, title, series_info, release_date, views
        FROM series
        WHERE series_id > $lastSeriesId
        ORDER BY series_id
        LIMIT $limit;`, prefix)

	writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())

	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			stmt, err := s.Prepare(ctx, query)
			if err != nil {
				return err
			}
			_, res, err = stmt.Execute(ctx, writeTx,
				table.NewQueryParameters(
					table.ValueParam("$limit", ydb.Uint64Value(limit)),
					table.ValueParam("$lastSeriesId", ydb.Uint64Value(lastSeries)),
				))
			return err
		}))
	return
}

func listByViews(ctx context.Context, sp *table.SessionPool, prefix string, limit uint64,
) (res *table.Result, err error) {
	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

        DECLARE $limit AS Uint64;

        $filter = (
            SELECT rev_views, series_id
            FROM series_rev_views
            ORDER BY rev_views, series_id
            LIMIT $limit
        );

        SELECT t2.series_id AS series_id, title, series_info, release_date, views
        FROM $filter AS t1
        INNER JOIN series AS t2 USING (series_id)
        ORDER BY views DESC, series_id ASC;`, prefix)

	writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())

	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			stmt, err := s.Prepare(ctx, query)
			if err != nil {
				return err
			}
			_, res, err = stmt.Execute(ctx, writeTx,
				table.NewQueryParameters(
					table.ValueParam("$limit", ydb.Uint64Value(limit)),
				))
			return err
		}))
	return
}

func listByViewsSeries(ctx context.Context, sp *table.SessionPool, prefix string, limit, lastSeries, lastViews uint64,
) (res *table.Result, err error) {
	query := fmt.Sprintf(`
        PRAGMA TablePathPrefix("%v");

        DECLARE $limit AS Uint64;
        DECLARE $lastSeriesId AS Uint64;
        DECLARE $lastViews AS Uint64;

        -- Simulate a DESC index by inverting views using max(uint64)-views
        $maxUint64 = 0xffffffffffffffff;
        $lastRevViews = $maxUint64 - $lastViews;

        $filterRaw = (
            SELECT rev_views, series_id
            FROM series_rev_views
            WHERE rev_views = $lastRevViews AND series_id > $lastSeriesId
            ORDER BY rev_views, series_id
            LIMIT $limit
            UNION ALL
            SELECT rev_views, series_id
            FROM series_rev_views
            WHERE rev_views > $lastRevViews
            ORDER BY rev_views, series_id
            LIMIT $limit
        );

        -- $filterRaw may have more than $limit rows
        $filter = (
            SELECT rev_views, series_id
            FROM $filterRaw
            ORDER BY rev_views, series_id
            LIMIT $limit
        );

        SELECT t2.series_id AS series_id, title, series_info, release_date, views
        FROM $filter AS t1
        INNER JOIN series AS t2 USING (series_id)
        ORDER BY views DESC, series_id ASC;`, prefix)

	writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())

	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			stmt, err := s.Prepare(ctx, query)
			if err != nil {
				return err
			}
			_, res, err = stmt.Execute(ctx, writeTx,
				table.NewQueryParameters(
					table.ValueParam("$limit", ydb.Uint64Value(limit)),
					table.ValueParam("$lastSeriesId", ydb.Uint64Value(lastSeries)),
					table.ValueParam("$lastViews", ydb.Uint64Value(lastViews)),
				))
			return err
		}))
	return
}

func parsListArgs(args []string) (r map[string]uint64, err error) {
	if len(args)%2 != 0 {
		err = errors.New("wrong usage of arguments")
		return
	}
	r = map[string]uint64{}
	for i := 0; i < len(args)-1; i += 2 {
		var n uint64
		n, err = strconv.ParseUint(args[i+1], 10, 64)
		if err != nil {
			return
		}
		r[args[i]] = n
	}
	if _, ok := r["limit"]; !ok {
		r["limit"] = 10
	}
	return
}

func printSeries(series SeriesList) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	defer w.Flush()

	_, _ = fmt.Fprintln(w, "series_id\ttitle\trelease_date\tinfo\tviews")
	for _, s := range series {
		_, _ = fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%d\t\n",
			s.ID,
			s.Title,
			s.ReleaseDate.Format(TimeISO8601),
			s.Info,
			s.Views,
		)
	}
	return nil
}

const TimeISO8601 = "2006-01-02"
