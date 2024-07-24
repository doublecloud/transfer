package registry

import (
	_ "github.com/doublecloud/tross/transfer_manager/go/pkg/transformer/registry/clickhouse"
	_ "github.com/doublecloud/tross/transfer_manager/go/pkg/transformer/registry/custom"
	_ "github.com/doublecloud/tross/transfer_manager/go/pkg/transformer/registry/filter"
	_ "github.com/doublecloud/tross/transfer_manager/go/pkg/transformer/registry/filter_rows"
	_ "github.com/doublecloud/tross/transfer_manager/go/pkg/transformer/registry/mask"
	_ "github.com/doublecloud/tross/transfer_manager/go/pkg/transformer/registry/number_to_float"
	_ "github.com/doublecloud/tross/transfer_manager/go/pkg/transformer/registry/raw_doc_grouper"
	_ "github.com/doublecloud/tross/transfer_manager/go/pkg/transformer/registry/rename"
	_ "github.com/doublecloud/tross/transfer_manager/go/pkg/transformer/registry/replace_primary_key"
	_ "github.com/doublecloud/tross/transfer_manager/go/pkg/transformer/registry/sharder"
	_ "github.com/doublecloud/tross/transfer_manager/go/pkg/transformer/registry/table_splitter"
	_ "github.com/doublecloud/tross/transfer_manager/go/pkg/transformer/registry/to_string"
)
