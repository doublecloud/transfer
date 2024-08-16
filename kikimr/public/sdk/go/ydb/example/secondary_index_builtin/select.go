package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"text/tabwriter"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/table"
)

func doSelect(
	ctx context.Context,
	sp *table.SessionPool,
	prefix string,
	args ...string,
) error {
	const query = `
		PRAGMA TablePathPrefix("%s");

		DECLARE $minViews AS Uint64;

		SELECT
			series_id,
			title,
			info,
			release_date,
			views,
			uploaded_user_id
		FROM
			` + "`series`" + `:views_index
		WHERE
			views >= $minViews
	`
	if len(args) == 0 {
		return fmt.Errorf("min views argument required")
	}
	minViews, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return err
	}

	res, err := execSelect(ctx, sp, fmt.Sprintf(query, prefix),
		table.ValueParam("$minViews", ydb.Uint64Value(minViews)),
	)
	if err != nil {
		return err
	}
	if !res.NextSet() {
		return nil
	}

	var series SeriesList
	err = (&series).Scan(res)
	if err != nil {
		return err
	}
	printSeries(series)

	return nil
}

func doSelectJoin(
	ctx context.Context,
	sp *table.SessionPool,
	prefix string,
	args ...string,
) error {
	const query = `
        PRAGMA TablePathPrefix("%s");

        DECLARE $userName AS Utf8;

        SELECT
			t1.series_id,
			t1.title,
			t1.info,
			t1.release_date,
			t1.views,
			t1.uploaded_user_id
        FROM
			` + "`series`" + `:users_index AS t1
        INNER JOIN
			` + "`users`" + `:name_index AS t2
			ON t1.uploaded_user_id == t2.user_id
        WHERE
			t2.name == $userName;
	`
	if len(args) == 0 {
		return fmt.Errorf("user name argument requiered")
	}
	userName := args[0]

	res, err := execSelect(ctx, sp, fmt.Sprintf(query, prefix),
		table.ValueParam("$userName", ydb.UTF8Value(userName)),
	)
	if err != nil {
		return err
	}
	if !res.NextSet() {
		return nil
	}

	var series SeriesList
	err = (&series).Scan(res)
	if err != nil {
		return err
	}
	printSeries(series)

	return nil
}

func execSelect(
	ctx context.Context, sp *table.SessionPool,
	query string, params ...table.ParameterOption,
) (
	res *table.Result, err error,
) {
	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			stmt, err := s.Prepare(ctx, query)
			if err != nil {
				return err
			}
			txc := table.TxControl(
				table.BeginTx(
					table.WithSerializableReadWrite(),
				),
				table.CommitTx(),
			)
			_, res, err = stmt.Execute(ctx, txc,
				table.NewQueryParameters(params...),
			)
			return err
		}),
	)
	return
}

func printSeries(series SeriesList) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	defer w.Flush()

	_, _ = fmt.Fprintln(w, "series_id\ttitle\trelease_date\tinfo\tviews\tuploaded_user_id")
	for _, s := range series {
		_, _ = fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%d\t%d\n",
			s.ID,
			s.Title,
			s.ReleaseDate.Format(TimeISO8601),
			s.Info,
			s.Views,
			s.UploadedUserID,
		)
	}
}
