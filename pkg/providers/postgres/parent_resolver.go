package postgres

import (
	"context"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/jackc/pgtype/pgxtype"
	"go.ytsaurus.tech/library/go/core/log"
)

func MakeChildParentMap(ctx context.Context, conn pgxtype.Querier) (map[abstract.TableID]abstract.TableID, error) {
	inheritRows, err := conn.Query(ctx, `select c.relname::text AS c_name, cs.nspname::text as c_schema,
       										   p.relname::text AS p_name, ps.nspname::text as  p_schema
										from pg_inherits
    									inner join pg_class as c on (pg_inherits.inhrelid=c.oid)
                						inner join pg_class as p on (pg_inherits.inhparent=p.oid)
										inner join pg_catalog.pg_namespace as ps on (p.relnamespace = ps.oid)
                						inner join pg_catalog.pg_namespace as cs on (c.relnamespace = cs.oid);`)
	if err != nil {
		logger.Log.Error("failed to execute SQL to list inherited tables", log.Error(err))
		return nil, xerrors.Errorf("failed to execute SQL to list inherited tables: %w", err)
	}
	defer inheritRows.Close()

	result := map[abstract.TableID]abstract.TableID{}
	for inheritRows.Next() {
		var child, childSchema, parent, parentSchema string
		if err := inheritRows.Scan(&child, &childSchema, &parent, &parentSchema); err != nil {
			return nil, err
		}
		childID := abstract.TableID{
			Namespace: childSchema,
			Name:      child,
		}

		parentID := abstract.TableID{
			Namespace: parentSchema,
			Name:      parent,
		}
		result[childID] = parentID
	}
	if err := inheritRows.Err(); err != nil {
		return nil, xerrors.Errorf("failed to get next row from inherited tables list query: %w", err)
	}

	return result, nil
}
