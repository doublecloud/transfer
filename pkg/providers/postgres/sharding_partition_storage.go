package postgres

import (
	"context"
	"fmt"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
)

func (s *Storage) getChildTables(ctx context.Context, table abstract.TableDescription) ([]abstract.TableDescription, error) {
	conn, err := s.Conn.Acquire(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to acquire a connection from pool: %w", err)
	}
	defer conn.Release()
	rows, err := conn.Query(ctx, `
SELECT
    c.oid::regclass::text as table_name,
    c.reltuples as eta_rows
FROM pg_class c
     INNER JOIN pg_inherits par on c.oid = par.inhrelid
WHERE
    par.inhparent :: regclass = $1 :: regclass;;
`, table.Fqtn())
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve child tables: %w", err)
	}
	var res []abstract.TableDescription
	for rows.Next() {
		var tableName string
		var etaRow uint64
		if err := rows.Scan(&tableName, &etaRow); err != nil {
			return nil, xerrors.Errorf("unable to scan row: %w", err)
		}
		tid, _ := abstract.ParseTableID(tableName)
		res = append(res, abstract.TableDescription{
			Name:   table.Name,
			Schema: table.Schema,
			Filter: abstract.WhereStatement(fmt.Sprintf("%s%s", PartitionsFilterPrefix, tid.Fqtn())),
			EtaRow: etaRow,
			Offset: 0,
		})
	}

	return res, nil
}
