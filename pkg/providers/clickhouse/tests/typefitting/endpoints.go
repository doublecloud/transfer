package typefitting

import (
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	chrecipe "github.com/doublecloud/transfer/pkg/providers/clickhouse/recipe"
)

var (
	//nolint:exhaustivestruct
	source = server.MockSource{}
	target = *chrecipe.MustTarget(chrecipe.WithDatabase("test"), chrecipe.WithInitFile("init.sql"))
)

func init() {
	source.WithDefaults()
	target.WithDefaults()
}
