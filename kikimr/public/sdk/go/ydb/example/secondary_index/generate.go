package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/table"
)

func doGenerate(
	ctx context.Context,
	sp *table.SessionPool,
	prefix string,
	args ...string,
) error {
	arg, err := parsGenerateArgs(args)
	if err != nil || arg == nil {
		return fmt.Errorf(`wrong arguments. Usage: start 1 count 2 threads 3.
		    start - First id to generate
    		count - Number of series to generate
    		threads - Number of threads to use
			Error: %v`, err)
	}

	jobs := make(chan Series, arg.count)
	result := make(chan error, arg.count)
	for i := 0; i < arg.threads; i++ {
		go insertSeriesWorker(ctx, sp, prefix, jobs, result)
	}

	for i, id := 0, arg.seriesID; i < arg.count; i, id = i+1, id+1 {
		jobs <- Series{
			ID:          id,
			Title:       fmt.Sprintf("Name %v", arg.seriesID),
			Info:        fmt.Sprintf("Info %v", arg.seriesID),
			ReleaseDate: time.Now(),
			Views:       uint64(rand.Int63n(1000000)),
		}
	}
	close(jobs)

	generated := 0
	for ; generated < arg.count; generated++ {
		if err = <-result; err != nil {
			break
		}
	}

	fmt.Printf("Generated %v new series", generated)
	return err
}

func insertSeriesWorker(ctx context.Context, sp *table.SessionPool, prefix string, jobs <-chan Series,
	err chan<- error) {
	query := fmt.Sprintf(`
        PRAGMA TablePathPrefix("%v");

		DECLARE $seriesId AS Uint64;
        DECLARE $title AS Utf8;
        DECLARE $seriesInfo AS Utf8;
        DECLARE $releaseDate AS DateTime;
        DECLARE $views AS Uint64;

        -- Simulate a DESC index by inverting views using max(uint64)-views
        $maxUint64 = 0xffffffffffffffff;
        $revViews = $maxUint64 - $views;

        INSERT INTO series (series_id, title, series_info, release_date, views)
        VALUES ($seriesId, $title, $seriesInfo, $releaseDate, $views);

        -- Insert above already verified series_id is unique, so it is safe to use upsert
        UPSERT INTO series_rev_views (rev_views, series_id)
        VALUES ($revViews, $seriesId);`, prefix)

	for j := range jobs {
		writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())
		err <- table.Retry(ctx, sp,
			table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
				stmt, err := s.Prepare(ctx, query)
				if err != nil {
					return err
				}

				_, _, err = stmt.Execute(ctx, writeTx,
					table.NewQueryParameters(
						table.ValueParam("$seriesId", ydb.Uint64Value(j.ID)),
						table.ValueParam("$title", ydb.UTF8Value(j.Title)),
						table.ValueParam("$seriesInfo", ydb.UTF8Value(j.Info)),
						table.ValueParam("$releaseDate", ydb.DatetimeValue(ydb.Time(j.ReleaseDate).Datetime())),
						table.ValueParam("$views", ydb.Uint64Value(j.Views)),
					))
				return err
			}))
	}
}

func parsGenerateArgs(args []string) (r *genArgs, err error) {
	if len(args)%2 != 0 {
		err = errors.New("wrong usage of arguments")
		return
	}
	r = &genArgs{
		seriesID: 1,
		count:    10,
		threads:  10,
	}
	for i := 0; i < len(args)-1; i += 2 {
		var n int
		n, err = strconv.Atoi(args[i+1])
		if err != nil {
			return
		}
		switch args[i] {
		case "start":
			r.seriesID = uint64(n)
		case "count":
			r.count = n
		case "threads":
			r.threads = n
		default:
			err = errors.New("wrong usage of arguments")
			return
		}
	}
	return
}

type genArgs struct {
	seriesID uint64
	count    int
	threads  int
}
