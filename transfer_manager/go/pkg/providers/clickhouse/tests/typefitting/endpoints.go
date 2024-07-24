package typefitting

import (
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	chrecipe "github.com/doublecloud/tross/transfer_manager/go/pkg/providers/clickhouse/recipe"
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
