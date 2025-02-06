package mysql

import (
	"context"
	"fmt"
	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
)

func (s *Storage) ShardTable(ctx context.Context, table abstract.TableDescription) ([]abstract.TableDescription, error) {
	partitions, err := s.resolvePartitions(ctx, table)
	if err != nil {
		logger.Log.Warnf("unable to load child tables: %v", err)
	}
	if len(partitions) > 0 {
		return partitions, nil
	}

	return []abstract.TableDescription{table}, nil
}

func (s *Storage) resolvePartitions(ctx context.Context, table abstract.TableDescription) ([]abstract.TableDescription, error) {
	rows, err := s.DB.QueryContext(
		ctx,
		`
SELECT PARTITION_NAME, TABLE_ROWS
FROM INFORMATION_SCHEMA.PARTITIONS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?;
`,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve partitions: %w", err)
	}
	var res []abstract.TableDescription
	for rows.Next() {
		var partName string
		var tableRows int
		if err := rows.Scan(&partName, &tableRows); err != nil {
			return nil, xerrors.Errorf("unable to scan partition: %w", err)
		}
		res = append(res, abstract.TableDescription{
			Name:   table.Name,
			Schema: table.Schema,
			Filter: abstract.WhereStatement(fmt.Sprintf("PARTITION (%s)", partName)),
			EtaRow: uint64(tableRows),
			Offset: 0,
		})
	}
	return res, nil
}
