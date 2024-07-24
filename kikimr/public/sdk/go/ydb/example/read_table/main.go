package main

import "github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/example/internal/cli"

func main() {
	cli.Run(new(Command))
}
