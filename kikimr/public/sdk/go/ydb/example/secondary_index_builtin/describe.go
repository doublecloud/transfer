package main

import (
	"context"
	"encoding/json"
	"fmt"
	"path"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/table"
)

func doDescribe(
	ctx context.Context,
	sp *table.SessionPool,
	prefix string,
	args ...string,
) error {
	for _, name := range []string{"series", "users"} {
		var desc table.Description
		err := table.Retry(ctx, sp,
			table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
				desc, err = s.DescribeTable(ctx, path.Join(prefix, name))
				return
			}),
		)
		if err != nil {
			return err
		}
		p, err := json.MarshalIndent(desc, "", "  ")
		if err != nil {
			return err
		}
		fmt.Printf("Describe %v table: %s \n", name, p)
	}
	return nil
}
