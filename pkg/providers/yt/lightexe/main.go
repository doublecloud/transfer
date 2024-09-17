package main

import (
	"os"

	ytmerge "github.com/doublecloud/transfer/pkg/providers/yt/mergejob"
	"go.ytsaurus.tech/yt/go/mapreduce"
)

func init() {
	mapreduce.Register(&ytmerge.MergeWithDeduplicationJob{
		Untyped: mapreduce.Untyped{},
	})
}

func main() {
	if mapreduce.InsideJob() {
		os.Exit(mapreduce.JobMain())
	}
}
