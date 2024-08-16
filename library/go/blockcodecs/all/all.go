package all

import (
	_ "github.com/doublecloud/transfer/library/go/blockcodecs/blockbrotli"
	_ "github.com/doublecloud/transfer/library/go/blockcodecs/blocklz4"
	_ "github.com/doublecloud/transfer/library/go/blockcodecs/blocksnappy"
	_ "github.com/doublecloud/transfer/library/go/blockcodecs/blockzstd"
)
