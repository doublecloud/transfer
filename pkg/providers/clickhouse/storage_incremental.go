package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
)

var (
	_ abstract.IncrementalStorage = new(Storage)
)

func (s *Storage) GetIncrementalState(ctx context.Context, incremental []abstract.IncrementalTable) ([]abstract.TableDescription, error) {
	var res []abstract.TableDescription
	for _, table := range incremental {
		maxVal, err := getMaxCursorFieldValue(ctx, s.db, table)
		if err != nil {
			return nil, err
		}

		res = append(res, abstract.TableDescription{
			Name:   table.Name,
			Schema: table.Namespace,
			Filter: abstract.WhereStatement(fmt.Sprintf(`"%s" > parseDateTime64BestEffort('%s', 9)`, table.CursorField, maxVal)),
			EtaRow: 0,
			Offset: 0,
		})
	}
	return res, nil
}

func (s *Storage) SetInitialState(tables []abstract.TableDescription, incrementalTables []abstract.IncrementalTable) {
	setInitialState(tables, incrementalTables)
}

func getMaxCursorFieldValue(ctx context.Context, db *sql.DB, table abstract.IncrementalTable) (interface{}, error) {
	var maxVal interface{}
	if err := db.QueryRowContext(
		ctx,
		fmt.Sprintf(
			`select toString(max(%s)) from "%s"."%s";`,
			table.CursorField,
			table.Namespace,
			table.Name,
		),
	).Scan(&maxVal); err != nil {
		return nil, xerrors.Errorf("unable get max %s from table: %s: %w", table.CursorField, table.TableID(), err)
	}
	var colTyp string
	if err := db.QueryRowContext(
		ctx,
		`
select type from system.columns
where table = ? and name = ?`,
		table.Name,
		table.CursorField,
	).Scan(&colTyp); err != nil {
		return nil, xerrors.Errorf("unable get max %s from table: %s: %w", table.CursorField, table.TableID(), err)
	}
	if !strings.Contains(colTyp, "Date") {
		return nil, abstract.NewFatalError(xerrors.Errorf("unable to get incremental col for %s col (only Date-like type supported)", colTyp))
	}

	return maxVal, nil
}

func setInitialState(tables []abstract.TableDescription, incrementalTables []abstract.IncrementalTable) {
	for i, tdesc := range tables {
		if tdesc.Filter != "" || tdesc.Offset != 0 {
			// tdesc already contains predicate
			continue
		}
		for _, incremental := range incrementalTables {
			if incremental.CursorField == "" || incremental.InitialState == "" {
				continue
			}
			if tdesc.ID() == incremental.TableID() {
				tables[i] = abstract.TableDescription{
					Name:   incremental.Name,
					Schema: incremental.Namespace,
					Filter: abstract.WhereStatement(fmt.Sprintf(`"%s" > parseDateTime64BestEffort('%s', 9)`, incremental.CursorField, incremental.InitialState)),
					EtaRow: 0,
					Offset: 0,
				}
			}
		}
	}
}
