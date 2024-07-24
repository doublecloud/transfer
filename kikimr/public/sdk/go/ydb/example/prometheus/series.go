package main

import (
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"path"
	"runtime"
	"sync"
	"text/template"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/connect"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/example/internal/cli"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/table"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/ydbsql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type templateConfig struct {
	TablePathPrefix string
}

var fill = template.Must(template.New("fill database").Parse(`
PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

DECLARE $seriesData AS List<Struct<
	series_id: Uint64,
	title: Utf8,
	series_info: Utf8,
	release_date: Date,
	comment: Optional<Utf8>>>;

DECLARE $seasonsData AS List<Struct<
	series_id: Uint64,
	season_id: Uint64,
	title: Utf8,
	first_aired: Date,
	last_aired: Date>>;

DECLARE $episodesData AS List<Struct<
	series_id: Uint64,
	season_id: Uint64,
	episode_id: Uint64,
	title: Utf8,
	air_date: Date>>;

REPLACE INTO series
SELECT
	series_id,
	title,
	series_info,
	CAST(release_date AS Uint64) AS release_date,
	comment
FROM AS_TABLE($seriesData);

REPLACE INTO seasons
SELECT
	series_id,
	season_id,
	title,
	CAST(first_aired AS Uint64) AS first_aired,
	CAST(last_aired AS Uint64) AS last_aired
FROM AS_TABLE($seasonsData);

REPLACE INTO episodes
SELECT
	series_id,
	season_id,
	episode_id,
	title,
	CAST(air_date AS Uint64) AS air_date
FROM AS_TABLE($episodesData);
`))

type Command struct {
}

func (cmd *Command) ExportFlags(context.Context, *flag.FlagSet) {}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) (err error) {
	go func() {
		goroutines := callGauges(prometheus.DefaultRegisterer, "goroutines")
		memory := callGauges(prometheus.DefaultRegisterer, "memory")
		uptime := callGauges(prometheus.DefaultRegisterer, "uptime")
		var stats runtime.MemStats
		start := time.Now()
		for {
			time.Sleep(time.Second)
			uptime.start().syncWithValue(nil, time.Since(start).Seconds())
			goroutines.start().syncWithValue(nil, float64(runtime.NumGoroutine()))
			runtime.ReadMemStats(&stats)
			memory.start().syncWithValue(nil, float64(stats.Alloc))
		}
	}()
	{
		db, err := connect.New(
			ydb.WithDriverTrace(
				table.WithClientTrace(
					table.WithSessionPoolTrace(
						ctx,
						tablePoolTrace(prometheus.DefaultRegisterer),
					),
					tableClientTrace(prometheus.DefaultRegisterer),
				),
				driverTrace(prometheus.DefaultRegisterer),
			),
			params.ConnectParams,
			connect.WithSessionPoolIdleThreshold(time.Second*5),
			connect.WithSessionPoolKeepAliveMinSize(-1),
		)
		if err != nil {
			return fmt.Errorf("connect error: %w", err)
		}

		go func() {
			log.Fatal(http.ListenAndServe(":8080", promhttp.Handler()))
		}()

		err = db.CleanupDatabase(ctx, params.Prefix(), "series", "episodes", "seasons")
		if err != nil {
			fmt.Printf("cleanup faled: %v\n", err)
		}

		err = db.EnsurePathExists(ctx, params.Prefix())
		if err != nil {
			fmt.Printf("ensure path exists check faled: %v\n", err)
		}

		err = describeTableOptions(ctx, db.Table().Pool())
		if err != nil {
			fmt.Printf("describe table options error: %b\n", err)
		}

		err = createTables(ctx, db.Table().Pool(), params.Prefix())
		if err != nil {
			fmt.Printf("create tables error: %v\n", err)
		}

		err = describeTable(ctx, db.Table().Pool(), path.Join(
			params.Prefix(), "series",
		))
		if err != nil {
			fmt.Printf("describe table error: %v\n", err)
		}

		err = fillTablesWithData(ctx, db.Table().Pool(), params.Prefix())
		if err != nil {
			fmt.Printf("describe table error: %v\n", err)
		}

		wg := sync.WaitGroup{}
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err = selectSimple(ctx, db.Table().Pool(), params.Prefix())
				if err != nil {
					fmt.Printf("select simple error: %v\n", err)
				}

				err = scanQuerySelect(ctx, db.Table().Pool(), params.Prefix())
				if err != nil {
					fmt.Printf("scan query select error: %v\n", err)
				}

				err = readTable(ctx, db.Table().Pool(), path.Join(
					params.Prefix(), "series",
				))
				if err != nil {
					fmt.Printf("read table error: %v\n", err)
				}
			}()
		}
		wg.Wait()
		db.Close()
	}
	{
		db := sql.OpenDB(
			ydbsql.Connector(
				ydbsql.WithDriverTrace(
					driverTrace(prometheus.DefaultRegisterer),
				),
				ydbsql.WithClientTrace(
					tableClientTrace(prometheus.DefaultRegisterer),
				),
				ydbsql.WithSessionPoolTrace(
					tablePoolTrace(prometheus.DefaultRegisterer),
				),
				ydbsql.WithEndpoint(params.ConnectParams.Endpoint()),
				ydbsql.WithDatabase(params.ConnectParams.Database()),
				ydbsql.WithSessionPoolIdleThreshold(time.Second*5),
			),
		)

		query := render(
			template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
			DECLARE $seriesID AS Uint64;
			$format = DateTime::Format("%Y-%m-%d");
			SELECT
				series_id,
				title,
				$format(DateTime::FromSeconds(CAST(DateTime::ToSeconds(DateTime::IntervalFromDays(CAST(release_date AS Int16))) AS Uint32))) AS release_date
			FROM
				series
			WHERE
				series_id = $seriesID;
		`)),
			templateConfig{
				TablePathPrefix: params.Prefix(),
			},
		)

		wg := sync.WaitGroup{}
		call := callGauges(prometheus.DefaultRegisterer, "app", TagMethod)
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var (
					err   error
					rows  *sql.Rows
					id    uint64
					title string
					date  []byte
				)
				for {
					scan := rand.Uint64()%2 != 0
					method := Label{
						Tag:   TagMethod,
						Value: ifStr(scan, "scan", "data"),
					}
					start := call.start(method)
					rows, err = db.QueryContext(
						func() context.Context {
							if scan {
								return ydbsql.WithScanQuery(ctx)
							}
							return ctx
						}(),
						query,
						sql.Named("seriesID", uint64(1)),
					)
					start.sync(err, method)
					if err != nil {
						fmt.Printf("query failed: %v\n", err)
						continue
					}
					for rows.Next() {
						if err = rows.Scan(&id, &title, &date); err != nil {
							fmt.Printf("scan failed: %v\n", err)
						}
					}
					if err := rows.Close(); err != nil {
						fmt.Printf("close db/sql failed: %v\n", err)
					}
				}
			}()
		}
		wg.Wait()

		if err := db.Close(); err != nil {
			fmt.Printf("close db/sql failed: %v\n", err)
		}
	}

	return nil
}

func readTable(ctx context.Context, sp *table.SessionPool, path string) (err error) {
	var res *table.Result
	return table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			res, err = s.StreamReadTable(ctx, path,
				table.ReadOrdered(),
				table.ReadColumn("series_id"),
				table.ReadColumn("title"),
				table.ReadColumn("release_date"),
			)
			if err != nil {
				return err
			}
			var (
				id    *uint64
				title *string
				date  *uint64
			)
			for res.NextResultSet(ctx, "series_id", "title", "release_date") {
				for res.NextRow() {
					err = res.Scan(&id, &title, &date)
					if err != nil {
						return err
					}
				}
			}
			if err := res.Err(); err != nil {
				return err
			}
			stats := res.Stats()
			for i := 0; ; i++ {
				phase, ok := stats.NextPhase()
				if !ok {
					break
				}
				for {
					_, ok := phase.NextTableAccess()
					if !ok {
						break
					}
				}
			}
			return res.Err()
		}),
	)
}

func describeTableOptions(ctx context.Context, sp *table.SessionPool) (err error) {
	return table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			_, err = s.DescribeTableOptions(ctx)
			return
		}),
	)
}

func selectSimple(ctx context.Context, sp *table.SessionPool, prefix string) (err error) {
	query := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
			DECLARE $seriesID AS Uint64;
			$format = DateTime::Format("%Y-%m-%d");
			SELECT
				series_id,
				title,
				$format(DateTime::FromSeconds(CAST(DateTime::ToSeconds(DateTime::IntervalFromDays(CAST(release_date AS Int16))) AS Uint32))) AS release_date
			FROM
				series
			WHERE
				series_id = $seriesID;
		`)),
		templateConfig{
			TablePathPrefix: prefix,
		},
	)
	readTx := table.TxControl(
		table.BeginTx(
			table.WithOnlineReadOnly(),
		),
		table.CommitTx(),
	)
	return table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			var res *table.Result
			_, res, err = s.Execute(ctx, readTx, query,
				table.NewQueryParameters(
					table.ValueParam("$seriesID", ydb.Uint64Value(1)),
				),
				table.WithQueryCachePolicy(
					table.WithQueryCachePolicyKeepInCache(),
				),
				table.WithCollectStatsModeBasic(),
			)
			if err != nil {
				return err
			}

			var (
				id    *uint64
				title *string
				date  *[]byte
			)

			for res.NextResultSet(ctx, "series_id", "title", "release_date") {
				for res.NextRow() {
					err = res.Scan(&id, &title, &date)
					if err != nil {
						return err
					}
				}
			}
			return res.Err()
		}),
	)
}

func scanQuerySelect(ctx context.Context, sp *table.SessionPool, prefix string) (err error) {
	query := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

			DECLARE $series AS List<UInt64>;

			SELECT series_id, season_id, title, CAST(CAST(first_aired AS Date) AS String) AS first_aired
			FROM seasons
			WHERE series_id IN $series
		`)),
		templateConfig{
			TablePathPrefix: prefix,
		},
	)

	var res *table.Result
	return table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			res, err = s.StreamExecuteScanQuery(ctx, query,
				table.NewQueryParameters(
					table.ValueParam("$series",
						ydb.ListValue(
							ydb.Uint64Value(1),
							ydb.Uint64Value(10),
						),
					),
				),
			)
			if err != nil {
				return err
			}
			var (
				seriesID uint64
				seasonID uint64
				title    string
				date     string // due to cast in select query
			)
			for res.NextResultSet(ctx) {
				for res.NextRow() {
					err = res.ScanWithDefaults(&seriesID, &seasonID, &title, &date)
					if err != nil {
						return err
					}
				}
			}
			return res.Err()
		}),
	)
}

func fillTablesWithData(ctx context.Context, sp *table.SessionPool, prefix string) (err error) {
	// Prepare write transaction.
	writeTx := table.TxControl(
		table.BeginTx(
			table.WithSerializableReadWrite(),
		),
		table.CommitTx(),
	)
	return table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			stmt, err := s.Prepare(ctx, render(fill, templateConfig{
				TablePathPrefix: prefix,
			}))
			if err != nil {
				return err
			}
			_, _, err = stmt.Execute(ctx, writeTx, table.NewQueryParameters(
				table.ValueParam("$seriesData", getSeriesData()),
				table.ValueParam("$seasonsData", getSeasonsData()),
				table.ValueParam("$episodesData", getEpisodesData()),
			))
			return err
		}),
	)
}

func createTables(ctx context.Context, sp *table.SessionPool, prefix string) (err error) {
	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			return s.CreateTable(ctx, path.Join(prefix, "series"),
				table.WithColumn("series_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("title", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("series_info", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("release_date", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("comment", ydb.Optional(ydb.TypeUTF8)),
				table.WithPrimaryKeyColumn("series_id"),
			)
		}),
	)
	if err != nil {
		return err
	}

	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			return s.CreateTable(ctx, path.Join(prefix, "seasons"),
				table.WithColumn("series_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("season_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("title", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("first_aired", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("last_aired", ydb.Optional(ydb.TypeUint64)),
				table.WithPrimaryKeyColumn("series_id", "season_id"),
			)
		}),
	)
	if err != nil {
		return err
	}

	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			return s.CreateTable(ctx, path.Join(prefix, "episodes"),
				table.WithColumn("series_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("season_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("episode_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("title", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("air_date", ydb.Optional(ydb.TypeUint64)),
				table.WithPrimaryKeyColumn("series_id", "season_id", "episode_id"),
			)
		}),
	)
	if err != nil {
		return err
	}

	return nil
}

func describeTable(ctx context.Context, sp *table.SessionPool, path string) (err error) {
	return table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			_, err = s.DescribeTable(ctx, path)
			return
		}),
	)
}

func render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	err := t.Execute(&buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}
