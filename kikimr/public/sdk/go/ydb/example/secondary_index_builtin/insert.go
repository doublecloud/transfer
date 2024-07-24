package main

import (
	"context"
	"fmt"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/table"
)

func doInsert(
	ctx context.Context,
	sp *table.SessionPool,
	prefix string,
	args ...string,
) error {
	const query = `
		PRAGMA TablePathPrefix("%s");

		DECLARE $seriesData AS List<Struct<
		    series_id: Uint64?,
		    title: Utf8?,
		    info: Utf8?,
		    release_date: Datetime?,
		    views: Uint64?,
		    uploaded_user_id: Uint64?>>;

		DECLARE $usersData AS List<Struct<
		    user_id: Uint64?,
		    name: Utf8?,
		    age: Uint32?>>;


		REPLACE INTO series
		SELECT
		    series_id,
		    title,
		    info,
		    release_date,
		    views,
		    uploaded_user_id
		FROM AS_TABLE($seriesData);

		REPLACE INTO users
		SELECT
		    user_id,
		    name,
		    age
		FROM AS_TABLE($usersData);
	`
	var (
		txc = table.TxControl(
			table.BeginTx(
				table.WithSerializableReadWrite(),
			),
			table.CommitTx(),
		)
		q = fmt.Sprintf(query, prefix)
	)
	return table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			_, _, err := s.Execute(ctx, txc, q,
				table.NewQueryParameters(
					table.ValueParam("$seriesData", series.ListValue()),
					table.ValueParam("$usersData", users.ListValue()),
				),
			)
			return err
		}),
	)
}

var series = SeriesList{
	{
		ID:             1,
		Title:          "First episode",
		ReleaseDate:    parseISO8601("2006-01-01"),
		Info:           "Pilot episode.",
		Views:          1000,
		UploadedUserID: 0,
	},
	{
		ID:             2,
		Title:          "Second episode",
		ReleaseDate:    parseISO8601("2006-02-01"),
		Info:           "Jon Snow knows nothing.",
		Views:          2000,
		UploadedUserID: 1,
	},
	{
		ID:             3,
		Title:          "Third episode",
		ReleaseDate:    parseISO8601("2006-03-01"),
		Info:           "Daenerys is the mother of dragons.",
		Views:          3000,
		UploadedUserID: 2,
	},
	{
		ID:             4,
		Title:          "Fourth episode",
		ReleaseDate:    parseISO8601("2006-04-01"),
		Info:           "Jorah Mormont is the king of the friendzone.",
		Views:          4000,
		UploadedUserID: 3,
	},
	{
		ID:             5,
		Title:          "Fifth episode",
		ReleaseDate:    parseISO8601("2006-05-01"),
		Info:           "Cercei is not good person.",
		Views:          5000,
		UploadedUserID: 1,
	},
	{
		ID:             6,
		Title:          "Sixth episode",
		ReleaseDate:    parseISO8601("2006-06-01"),
		Info:           "Tyrion is not big.",
		Views:          6000,
		UploadedUserID: 2,
	},
	{
		ID:             7,
		Title:          "Seventh episode",
		ReleaseDate:    parseISO8601("2006-07-01"),
		Info:           "Tywin should close the door.",
		Views:          7000,
		UploadedUserID: 2,
	},
	{
		ID:             8,
		Title:          "Eighth episode",
		ReleaseDate:    parseISO8601("2006-08-01"),
		Info:           "The white walkers are well-organized.",
		Views:          8000,
		UploadedUserID: 3,
	},
	{
		ID:             9,
		Title:          "Ninth episode",
		ReleaseDate:    parseISO8601("2006-09-01"),
		Info:           "Dragons can fly.",
		Views:          9000,
		UploadedUserID: 1,
	},
}

var users = UsersList{
	{
		ID:   0,
		Name: "Kit Harrington",
		Age:  32,
	},
	{
		ID:   1,
		Name: "Emilia Clarke",
		Age:  32,
	},
	{
		ID:   2,
		Name: "Jason Momoa",
		Age:  39,
	},
	{
		ID:   3,
		Name: "Peter Dinklage",
		Age:  49,
	},
}

const TimeISO8601 = "2006-01-02"

func parseISO8601(date string) time.Time {
	t, err := time.Parse(TimeISO8601, date)
	if err != nil {
		panic(err)
	}
	return t
}
