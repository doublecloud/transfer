package dbt

import (
	"fmt"
	"os"
	"testing"

	"github.com/doublecloud/transfer/library/go/test/yatest"
	"github.com/doublecloud/transfer/pkg/abstract"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	chrecipe "github.com/doublecloud/transfer/pkg/providers/clickhouse/recipe"
	"github.com/doublecloud/transfer/pkg/providers/postgres/pgrecipe"
	"github.com/doublecloud/transfer/pkg/runtime/shared/pod"
	transformers_registry "github.com/doublecloud/transfer/pkg/transformer"
	"github.com/doublecloud/transfer/pkg/transformer/registry/dbt"
	_ "github.com/doublecloud/transfer/pkg/transformer/registry/dbt/clickhouse"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
)

func TestSnapshot(t *testing.T) {
	t.Skip()
	_ = os.Setenv("DBT_CONTAINER_REGISTRY", "12197361.preprod")
	_ = os.Setenv("DBT_IMAGE_TAG", "public.ecr.aws/t9p9v8b9")

	source := pgrecipe.RecipeSource(
		pgrecipe.WithInitFiles(yatest.SourcePath("transfer_manager/go/tests/e2e/pg2ch/dbt/init_pg.sql")),
		pgrecipe.WithoutPgDump(),
	)
	target := chrecipe.MustTarget(
		chrecipe.WithInitFile(yatest.SourcePath("transfer_manager/go/tests/e2e/pg2ch/dbt/init_ch.sql")),
		chrecipe.WithDatabase("dbttest"),
	)

	pod.SharedDir = "/tmp"

	githubPAT := os.Getenv("DOUBLECLOUD_GITHUB_PERSONAL_ACCESS_TOKEN")
	if githubPAT == "" {
		t.Skip("DOUBLECLOUD_GITHUB_PERSONAL_ACCESS_TOKEN not provided")
	}
	require.NotEmpty(t, githubPAT)

	// Source.WithDefaults() // has already been initialized by the `helpers` package
	target.WithDefaults()
	target.ProtocolUnspecified = true
	target.UseSchemaInTableName = true
	target.Cleanup = server.Drop
	transfer := helpers.MakeTransfer("testtransfer", source, target, abstract.TransferTypeSnapshotOnly)
	addTransformationToTransfer(transfer, dbt.Config{
		GitRepositoryLink: fmt.Sprintf("https://%s@github.com/doublecloud/tests-clickhouse-dbt.git", githubPAT),
		ProfileName:       "clickhouse",
		Operation:         "run",
	})

	_ = helpers.Activate(t, transfer)

	targetAsStorage := helpers.GetSampleableStorageByModel(t, target)
	targetTables, err := targetAsStorage.TableList(nil)
	require.NoError(t, err)
	require.Contains(t, targetTables, *abstract.NewTableID("dbttest", "v1"))
	require.Contains(t, targetTables, *abstract.NewTableID("dbttest", "v2"))
	require.Contains(t, targetTables, *abstract.NewTableID("dbttest", "v3"))
}

func addTransformationToTransfer(transfer *server.Transfer, config dbt.Config) {
	if transfer.Transformation == nil {
		transfer.Transformation = &server.Transformation{
			ExtraTransformers: nil,
			Executor:          nil,
		}
	}
	if transfer.Transformation.Transformers == nil {
		transfer.Transformation.Transformers = new(transformers_registry.Transformers)
	}
	transfer.Transformation.Transformers.Transformers = append(transfer.Transformation.Transformers.Transformers, transformers_registry.Transformer{
		dbt.TransformerType: config,
	})
}
