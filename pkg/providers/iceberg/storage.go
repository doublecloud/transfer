package iceberg

import (
	"context"
	"github.com/apache/iceberg-go/catalog/glue"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/doublecloud/transfer/pkg/abstract/changeitem"
	"strings"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/apache/iceberg-go"
	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/typesystem"
	"go.ytsaurus.tech/library/go/core/log"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

// To verify providers contract implementation
var (
	_ abstract.Storage = (*Storage)(nil)
)

// defaultReadBatchSize is magic number by in-leskin
// we need to push rather small chunks so our bufferer can buffer effectively
const defaultReadBatchSize = 128

type Storage struct {
	cfg      *IcebergSource
	logger   log.Logger
	registry metrics.Registry
	props    iceberg.Properties
	cat      catalog.Catalog
}

func (s *Storage) Close() {
}

func (s *Storage) Ping() error {
	return nil
}

func (s *Storage) LoadTable(ctx context.Context, tid abstract.TableDescription, pusher abstract.Pusher) error {
	tbl := table.Identifier{tid.Schema, tid.Name}
	itable, err := s.cat.LoadTable(ctx, tbl, s.props)
	if err != nil {
		return xerrors.Errorf("unable to load table: %v: %w", tbl, err)
	}
	_, arrowReadeer, err := itable.Scan().ToArrowRecords(ctx)
	if err != nil {
		return xerrors.Errorf("unable to read arrow table: %v: %w", tbl, err)
	}

	batch := make([]abstract.ChangeItem, 0, defaultReadBatchSize)
	tSchema, err := s.TableSchema(ctx, tid.ID())
	if err != nil {
		return xerrors.Errorf("unable to resolve schema: %w", err)
	}
	for page, err := range arrowReadeer {
		if err != nil {
			return xerrors.Errorf("unable to read record: %w", err)
		}
		for i := range page.NumRows() {
			if len(batch) == defaultReadBatchSize {
				if err := pusher(batch); err != nil {
					return xerrors.Errorf("unable to push batch: %w", err)
				}
				batch = make([]abstract.ChangeItem, 0, defaultReadBatchSize)
			}
			row := abstract.ChangeItem{
				ID:           uint32(itable.CurrentSnapshot().SnapshotID),
				LSN:          uint64(i),
				CommitTime:   uint64(itable.CurrentSnapshot().TimestampMs) * uint64(1000000),
				Counter:      int(i),
				Kind:         abstract.InsertKind,
				Schema:       tid.Schema,
				Table:        tid.Name,
				PartID:       "",
				ColumnNames:  tSchema.ColumnNames(),
				ColumnValues: make([]interface{}, len(tSchema.ColumnNames())),
				TableSchema:  tSchema,
				OldKeys:      changeitem.OldKeysType{},
				TxID:         "",
				Query:        "",
				Size:         changeitem.EventSize{},
			}
			for j := range page.NumCols() {
				if page.Column(int(j)).IsNull(int(i)) {
					continue
				}
				row.ColumnValues[j] = abstract.Restore(
					tSchema.Columns()[int(j)],
					page.Column(int(j)).GetOneForMarshal(int(i)),
				)
			}
			batch = append(batch, row)
		}
	}
	if len(batch) > 0 {
		if err := pusher(batch); err != nil {
			return xerrors.Errorf("unable to push batch: %w", err)
		}
	}
	return nil
}

func (s *Storage) TableSchema(ctx context.Context, tid abstract.TableID) (*abstract.TableSchema, error) {
	tbl := table.Identifier{tid.Namespace, tid.Name}
	itable, err := s.cat.LoadTable(ctx, tbl, s.props)
	if err != nil {
		return nil, xerrors.Errorf("unable to load table: %v: %w", tbl, err)
	}
	return s.FromIcebergSchema(itable.Schema()), nil
}

func (s *Storage) TableList(filter abstract.IncludeTableList) (abstract.TableMap, error) {
	tbls, err := s.cat.ListTables(context.TODO(), table.Identifier{s.cfg.Schema})
	if err != nil {
		return nil, xerrors.Errorf("unable to list table: %w", err)
	}
	res := abstract.TableMap{}
	for _, tbl := range tbls {
		if filter != nil && !filter.Include(s.AsTableID(tbl)) {
			continue
		}
		itable, err := s.cat.LoadTable(context.TODO(), tbl, s.props)
		if err != nil {
			return nil, xerrors.Errorf("unable to load table: %v: %w", tbl, err)
		}
		files, err := itable.Scan().PlanFiles(context.TODO())
		if err != nil {
			return nil, xerrors.Errorf("unable to plan files to read: %w", err)
		}
		totalCount := uint64(0)
		for _, file := range files {
			totalCount = totalCount + uint64(file.File.Count())
		}
		res[s.AsTableID(tbl)] = abstract.TableInfo{
			EtaRow: totalCount,
			IsView: false,
			Schema: s.FromIcebergSchema(itable.Schema()),
		}
	}
	return res, nil
}

func (s *Storage) ExactTableRowsCount(table abstract.TableID) (uint64, error) {
	return s.EstimateTableRowsCount(table)
}

func (s *Storage) EstimateTableRowsCount(tid abstract.TableID) (uint64, error) {
	tbl := table.Identifier{tid.Namespace, tid.Name}
	itable, err := s.cat.LoadTable(context.TODO(), tbl, s.props)
	if err != nil {
		return 0, xerrors.Errorf("unable to load table: %v: %w", tbl, err)
	}
	files, err := itable.Scan().PlanFiles(context.TODO())
	if err != nil {
		return 0, xerrors.Errorf("unable to plan files to read: %w", err)
	}
	totalCount := uint64(0)
	for _, file := range files {
		totalCount = totalCount + uint64(file.File.Count())
	}
	return totalCount, nil
}

func (s *Storage) TableExists(tid abstract.TableID) (bool, error) {
	tbl := table.Identifier{tid.Namespace, tid.Name}
	_, err := s.cat.LoadTable(context.TODO(), tbl, s.props)
	if err != nil {
		return false, xerrors.Errorf("unable to load table: %v: %w", tbl, err)
	}
	return true, nil
}

func (s *Storage) AsTableID(tbl table.Identifier) abstract.TableID {
	if len(tbl) == 1 {
		return abstract.TableID{
			Namespace: "",
			Name:      tbl[0],
		}
	}
	if len(tbl) == 2 {
		return abstract.TableID{
			Namespace: tbl[0],
			Name:      tbl[1],
		}
	}
	return abstract.TableID{
		Namespace: strings.Join(tbl[:len(tbl)-1], "."),
		Name:      tbl[len(tbl)-1],
	}
}

func (s *Storage) FromIcebergSchema(schema *iceberg.Schema) *abstract.TableSchema {
	var cols []abstract.ColSchema
	for _, field := range schema.Fields() {
		isKey := false
		for _, id := range schema.IdentifierFieldIDs {
			if field.ID == id {
				isKey = true
			}
		}
		dtType := yt_schema.TypeAny
		if typ, ok := typesystem.RuleFor(ProviderType).Source[trimSuffix(field.Type.String())]; ok {
			dtType = typ
		}

		cols = append(cols, abstract.ColSchema{
			TableSchema:  "",
			TableName:    "",
			Path:         "",
			ColumnName:   field.Name,
			DataType:     dtType.String(),
			PrimaryKey:   isKey,
			FakeKey:      false,
			Required:     field.Required,
			Expression:   "",
			OriginalType: "",
			Properties:   nil,
		})
	}
	return abstract.NewTableSchema(cols)
}

func trimSuffix(s string) string {
	ss := strings.Split(s, "(")
	sss := strings.Split(ss[0], "[")
	return sss[0]
}

func NewStorage(src *IcebergSource, logger log.Logger, registry metrics.Registry) (*Storage, error) {
	var cat catalog.Catalog
	if src.CatalogType == "rest" {
		var err error
		cat, err = rest.NewCatalog(context.Background(), src.CatalogType, src.CatalogURI)
		if err != nil {
			return nil, xerrors.Errorf("unable to init catalog: %w", err)
		}
	} else if src.CatalogType == "glue" {
		cat = glue.NewCatalog()
	}
	return &Storage{
		cfg:      src,
		logger:   logger,
		registry: registry,
		props:    src.Properties,
		cat:      cat,
	}, nil
}
