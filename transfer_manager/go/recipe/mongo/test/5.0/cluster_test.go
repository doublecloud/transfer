package example

import (
	"testing"

	"github.com/doublecloud/transfer/transfer_manager/go/recipe/mongo/pkg/util"
)

func TestSample(t *testing.T) {
	util.TestMongoShardedClusterRecipe(t)
}
