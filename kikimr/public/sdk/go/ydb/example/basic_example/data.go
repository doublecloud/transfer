package main

import (
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
)

func seriesData(id uint64, released uint32, title, info, comment string) ydb.Value {
	var commentv ydb.Value
	if comment == "" {
		commentv = ydb.NullValue(ydb.TypeUTF8)
	} else {
		commentv = ydb.OptionalValue(ydb.UTF8Value(comment))
	}
	return ydb.StructValue(
		ydb.StructFieldValue("series_id", ydb.Uint64Value(id)),
		ydb.StructFieldValue("release_date", ydb.DateValue(released)),
		ydb.StructFieldValue("title", ydb.UTF8Value(title)),
		ydb.StructFieldValue("series_info", ydb.UTF8Value(info)),
		ydb.StructFieldValue("comment", commentv),
	)
}

func seasonData(seriesID, seasonID uint64, title string, first, last uint32) ydb.Value {
	return ydb.StructValue(
		ydb.StructFieldValue("series_id", ydb.Uint64Value(seriesID)),
		ydb.StructFieldValue("season_id", ydb.Uint64Value(seasonID)),
		ydb.StructFieldValue("title", ydb.UTF8Value(title)),
		ydb.StructFieldValue("first_aired", ydb.DateValue(first)),
		ydb.StructFieldValue("last_aired", ydb.DateValue(last)),
	)
}

func episodeData(seriesID, seasonID, episodeID uint64, title string, date uint32) ydb.Value {
	return ydb.StructValue(
		ydb.StructFieldValue("series_id", ydb.Uint64Value(seriesID)),
		ydb.StructFieldValue("season_id", ydb.Uint64Value(seasonID)),
		ydb.StructFieldValue("episode_id", ydb.Uint64Value(episodeID)),
		ydb.StructFieldValue("title", ydb.UTF8Value(title)),
		ydb.StructFieldValue("air_date", ydb.DateValue(date)),
	)
}

func getSeriesData() ydb.Value {
	return ydb.ListValue(
		seriesData(
			1, days("2006-02-03"), "IT Crowd", ""+
				"The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by "+
				"Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry.",
			"", // NULL comment.
		),
		seriesData(
			2, days("2014-04-06"), "Silicon Valley", ""+
				"Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and "+
				"Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.",
			"Some comment here",
		),
	)
}

func getSeasonsData() ydb.Value {
	return ydb.ListValue(
		seasonData(1, 1, "Season 1", days("2006-02-03"), days("2006-03-03")),
		seasonData(1, 2, "Season 2", days("2007-08-24"), days("2007-09-28")),
		seasonData(1, 3, "Season 3", days("2008-11-21"), days("2008-12-26")),
		seasonData(1, 4, "Season 4", days("2010-06-25"), days("2010-07-30")),
		seasonData(2, 1, "Season 1", days("2014-04-06"), days("2014-06-01")),
		seasonData(2, 2, "Season 2", days("2015-04-12"), days("2015-06-14")),
		seasonData(2, 3, "Season 3", days("2016-04-24"), days("2016-06-26")),
		seasonData(2, 4, "Season 4", days("2017-04-23"), days("2017-06-25")),
		seasonData(2, 5, "Season 5", days("2018-03-25"), days("2018-05-13")),
	)
}

func getEpisodesData() ydb.Value {
	return ydb.ListValue(
		episodeData(1, 1, 1, "Yesterday's Jam", days("2006-02-03")),
		episodeData(1, 1, 2, "Calamity Jen", days("2006-02-03")),
		episodeData(1, 1, 3, "Fifty-Fifty", days("2006-02-10")),
		episodeData(1, 1, 4, "The Red Door", days("2006-02-17")),
		episodeData(1, 1, 5, "The Haunting of Bill Crouse", days("2006-02-24")),
		episodeData(1, 1, 6, "Aunt Irma Visits", days("2006-03-03")),
		episodeData(1, 2, 1, "The Work Outing", days("2006-08-24")),
		episodeData(1, 2, 2, "Return of the Golden Child", days("2007-08-31")),
		episodeData(1, 2, 3, "Moss and the German", days("2007-09-07")),
		episodeData(1, 2, 4, "The Dinner Party", days("2007-09-14")),
		episodeData(1, 2, 5, "Smoke and Mirrors", days("2007-09-21")),
		episodeData(1, 2, 6, "Men Without Women", days("2007-09-28")),
		episodeData(1, 3, 1, "From Hell", days("2008-11-21")),
		episodeData(1, 3, 2, "Are We Not Men?", days("2008-11-28")),
		episodeData(1, 3, 3, "Tramps Like Us", days("2008-12-05")),
		episodeData(1, 3, 4, "The Speech", days("2008-12-12")),
		episodeData(1, 3, 5, "Friendface", days("2008-12-19")),
		episodeData(1, 3, 6, "Calendar Geeks", days("2008-12-26")),
		episodeData(1, 4, 1, "Jen The Fredo", days("2010-06-25")),
		episodeData(1, 4, 2, "The Final Countdown", days("2010-07-02")),
		episodeData(1, 4, 3, "Something Happened", days("2010-07-09")),
		episodeData(1, 4, 4, "Italian For Beginners", days("2010-07-16")),
		episodeData(1, 4, 5, "Bad Boys", days("2010-07-23")),
		episodeData(1, 4, 6, "Reynholm vs Reynholm", days("2010-07-30")),
		episodeData(2, 1, 1, "Minimum Viable Product", days("2014-04-06")),
		episodeData(2, 1, 2, "The Cap Table", days("2014-04-13")),
		episodeData(2, 1, 3, "Articles of Incorporation", days("2014-04-20")),
		episodeData(2, 1, 4, "Fiduciary Duties", days("2014-04-27")),
		episodeData(2, 1, 5, "Signaling Risk", days("2014-05-04")),
		episodeData(2, 1, 6, "Third Party Insourcing", days("2014-05-11")),
		episodeData(2, 1, 7, "Proof of Concept", days("2014-05-18")),
		episodeData(2, 1, 8, "Optimal Tip-to-Tip Efficiency", days("2014-06-01")),
		episodeData(2, 2, 1, "Sand Hill Shuffle", days("2015-04-12")),
		episodeData(2, 2, 2, "Runaway Devaluation", days("2015-04-19")),
		episodeData(2, 2, 3, "Bad Money", days("2015-04-26")),
		episodeData(2, 2, 4, "The Lady", days("2015-05-03")),
		episodeData(2, 2, 5, "Server Space", days("2015-05-10")),
		episodeData(2, 2, 6, "Homicide", days("2015-05-17")),
		episodeData(2, 2, 7, "Adult Content", days("2015-05-24")),
		episodeData(2, 2, 8, "White Hat/Black Hat", days("2015-05-31")),
		episodeData(2, 2, 9, "Binding Arbitration", days("2015-06-07")),
		episodeData(2, 2, 10, "Two Days of the Condor", days("2015-06-14")),
		episodeData(2, 3, 1, "Founder Friendly", days("2016-04-24")),
		episodeData(2, 3, 2, "Two in the Box", days("2016-05-01")),
		episodeData(2, 3, 3, "Meinertzhagen's Haversack", days("2016-05-08")),
		episodeData(2, 3, 4, "Maleant Data Systems Solutions", days("2016-05-15")),
		episodeData(2, 3, 5, "The Empty Chair", days("2016-05-22")),
		episodeData(2, 3, 6, "Bachmanity Insanity", days("2016-05-29")),
		episodeData(2, 3, 7, "To Build a Better Beta", days("2016-06-05")),
		episodeData(2, 3, 8, "Bachman's Earnings Over-Ride", days("2016-06-12")),
		episodeData(2, 3, 9, "Daily Active Users", days("2016-06-19")),
		episodeData(2, 3, 10, "The Uptick", days("2016-06-26")),
		episodeData(2, 4, 1, "Success Failure", days("2017-04-23")),
		episodeData(2, 4, 2, "Terms of Service", days("2017-04-30")),
		episodeData(2, 4, 3, "Intellectual Property", days("2017-05-07")),
		episodeData(2, 4, 4, "Teambuilding Exercise", days("2017-05-14")),
		episodeData(2, 4, 5, "The Blood Boy", days("2017-05-21")),
		episodeData(2, 4, 6, "Customer Service", days("2017-05-28")),
		episodeData(2, 4, 7, "The Patent Troll", days("2017-06-04")),
		episodeData(2, 4, 8, "The Keenan Vortex", days("2017-06-11")),
		episodeData(2, 4, 9, "Hooli-Con", days("2017-06-18")),
		episodeData(2, 4, 10, "Server Error", days("2017-06-25")),
		episodeData(2, 5, 1, "Grow Fast or Die Slow", days("2018-03-25")),
		episodeData(2, 5, 2, "Reorientation", days("2018-04-01")),
		episodeData(2, 5, 3, "Chief Operating Officer", days("2018-04-08")),
		episodeData(2, 5, 4, "Tech Evangelist", days("2018-04-15")),
		episodeData(2, 5, 5, "Facial Recognition", days("2018-04-22")),
		episodeData(2, 5, 6, "Artificial Emotional Intelligence", days("2018-04-29")),
		episodeData(2, 5, 7, "Initial Coin Offering", days("2018-05-06")),
		episodeData(2, 5, 8, "Fifty-One Percent", days("2018-05-13")),
	)
}

const DateISO8601 = "2006-01-02"

func days(date string) uint32 {
	t, err := time.Parse(DateISO8601, date)
	if err != nil {
		panic(err)
	}
	return ydb.Time(t).Date()
}
