package statictable

import (
	"fmt"
	"time"

	"github.com/doublecloud/transfer/pkg/abstract"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/exp/maps"
)

const (
	tmpNamePostfix    = "tmp"
	sortedNamePostfix = "sorted"

	retriesCount = 5
)

var (
	trueConst    = true
	subTxTimeout = yson.Duration(time.Minute * 5)
)

func makeTablePath(path ypath.Path, infix, postfix string) ypath.Path {
	return ypath.Path(fmt.Sprintf("%s_%s_%s", path.String(), infix, postfix))
}

func createNodeOptions(scheme schema.Schema, optimizeFor string, customAttributes map[string]any) yt.CreateNodeOptions {
	maps.Copy(customAttributes, map[string]any{
		"schema":       scheme,
		"optimize_for": optimizeFor,
		"strict":       true,
	})

	return yt.CreateNodeOptions{
		Attributes:     customAttributes,
		Recursive:      true,
		IgnoreExisting: false,
	}

}

func transactionOptions(id yt.TxID) *yt.TransactionOptions {
	return &yt.TransactionOptions{
		TransactionID: id,
		PingAncestors: true,
		Ping:          true,
	}
}

func makeYtSchema(scheme []abstract.ColSchema) schema.Schema {
	ytCols := abstract.ToYtSchema(scheme, false)
	return schema.Schema{
		Columns: ytCols,
		Strict:  &trueConst,
	}
}

func isSorted(scheme schema.Schema) bool {
	return len(scheme.KeyColumns()) > 0
}
