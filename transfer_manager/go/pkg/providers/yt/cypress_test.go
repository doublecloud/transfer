package yt

import (
	"context"
	"testing"

	ytclient "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yt/client"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/ypath"
	commonyt "go.ytsaurus.tech/yt/go/yt"
)

func TestSafeChildName(t *testing.T) {
	basePath := ypath.Path("//home/cdc/test/kry127")

	require.Equal(t,
		ypath.Path("//home/cdc/test/kry127"),
		SafeChild(basePath, ""),
	)
	require.Equal(t,
		ypath.Path("//home/cdc/test/kry127/basic/usage"),
		SafeChild(basePath, "basic/usage"),
	)
	require.Equal(t,
		ypath.Path("//home/cdc/test/kry127/My/Ydb/Table/Full/Path"),
		SafeChild(basePath, "/My/Ydb/Table/Full/Path"),
		"there should be one slash between correct ypath and appended table name",
	)
	require.Equal(t,
		ypath.Path("//home/cdc/test/kry127/weird/end/slash"),
		SafeChild(basePath, "weird/end/slash/"),
		"after appending table name with ending slash, it should be deleted to form correct ypath",
	)
	require.Equal(t,
		ypath.Path("//home/cdc/test/kry127/Weird/Slashes/Around"),
		SafeChild(basePath, "/Weird/Slashes/Around/"),
		"no slash doubling or ending should occur while appending table name with both beginning and ending slashes",
	)
	require.Equal(t,
		ypath.Path("//home/cdc/test/kry127/Middle/Slashes"),
		SafeChild(basePath, "Middle/////Slashes"),
		"slashes should be deduplicated",
	)
	require.Equal(t,
		ypath.Path("//home/cdc/test/kry127/Append/Multiple/Children/As/Relative/Path"),
		SafeChild(basePath, "Append///Multiple", "///Children///", "As", "/Relative/Path///"),
		"slashes should be deduplicated even when multiple children are appended",
	)
}

func TestListNodesWithAttrs(t *testing.T) {
	config := new(commonyt.Config)
	client, err := ytclient.NewYtClientWrapper(ytclient.HTTP, nil, config)
	require.NoError(t, err)
	defer client.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err = client.CreateNode(ctx, ypath.Path("//home/cdc/test"), commonyt.NodeMap, &commonyt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)

	_, err = client.CreateNode(ctx, ypath.Path("//home/cdc/test/node1"), commonyt.NodeTable, &commonyt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)

	_, err = client.CreateNode(ctx, ypath.Path("//home/cdc/test/node2"), commonyt.NodeTable, &commonyt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)

	_, err = client.CreateNode(ctx, ypath.Path("//home/cdc/test1/node1"), commonyt.NodeTable, &commonyt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)

	_, err = client.CreateNode(ctx, ypath.Path("//home/cdc/test1/node2"), commonyt.NodeTable, &commonyt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)

	nodes, err := ListNodesWithAttrs(ctx, client, ypath.Path("//home/cdc"), "test/node", true)
	require.NoError(t, err)
	require.Equal(t, len(nodes), 2)
	require.Equal(t, nodes[0].Name, "test/node1")
}
