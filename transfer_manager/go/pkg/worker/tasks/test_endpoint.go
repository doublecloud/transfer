package tasks

import (
	"context"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/errors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/errors/categories"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/middlewares"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/sink"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/storage"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/transformer"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/util"
)

type TestEndpointParams struct {
	Transfer             *model.Transfer
	TransformationConfig []byte
	Type                 abstract.ProviderType
	Params               string
	IsSource             bool
}

const (
	CredentialsCheckType   = abstract.CheckType("credentials")
	DataSampleCheckType    = abstract.CheckType("data-sample")
	ConfigCheckType        = abstract.CheckType("config-valid-check")
	InitCheckType          = abstract.CheckType("init-check")
	PingCheckType          = abstract.CheckType("ping-check")
	WriteableCheckType     = abstract.CheckType("writeable")
	LoadTablesCheckType    = abstract.CheckType("load-all-tables")
	EstimateTalbeCheckType = abstract.CheckType("estimate-table")
	LoadSampleCheckType    = abstract.CheckType("load-sample")
)

func SniffReplicationData(
	ctx context.Context,
	sniffer abstract.Fetchable,
	tr *abstract.TestResult,
	transfer *model.Transfer,
) *abstract.TestResult {
	res, err := sniffer.Fetch()
	if err != nil {
		return tr.NotOk(DataSampleCheckType, errors.CategorizedErrorf(categories.Source, "unable to read sample: %w", err))
	}
	previewMap := map[abstract.TableID][]abstract.ChangeItem{}
	for _, row := range res {
		previewMap[row.TableID()] = append(previewMap[row.TableID()], row)
	}
	if transfer.HasPublicTransformation() {
		for table, rows := range previewMap {
			collector := &sampleCollector{
				res:    map[abstract.TableID][]abstract.ChangeItem{},
				ctx:    context.Background(),
				cancel: func() {},
			}
			transfer.Transformation.Transformers.ErrorsOutput = &transformer.ErrorsOutput{
				// force sink error, so user can see all transformer errors in discover results
				Type:   transformer.SinkErrorsOutput,
				Config: nil,
			}
			wrapper, err := middlewares.Transformation(transfer, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
			if err != nil {
				return tr.NotOk(ConfigCheckType, xerrors.Errorf("unable to assign transformer: %w", err))
			}
			if err := wrapper(collector).Push(rows); err != nil {
				return tr.NotOk(DataSampleCheckType, xerrors.Errorf("unable to push items to transformation: %w", err))
			}
			previewMap[table] = collector.res[table]
		}
	}

	tr.Ok(ConfigCheckType)
	tr.Ok(DataSampleCheckType)
	tr.Ok(ConfigCheckType)

	tr.Preview = previewMap
	return tr
}

var _ abstract.Sinker = (*sampleCollector)(nil)

type sampleCollector struct {
	res    map[abstract.TableID][]abstract.ChangeItem
	ctx    context.Context
	cancel func()
}

func (s *sampleCollector) Close() error {
	return nil
}

func (s *sampleCollector) Push(items []abstract.ChangeItem) error {
	for _, item := range items {
		if len(s.res[item.TableID()]) > 3 {
			s.cancel()
			continue
		}
		if item.IsRowEvent() {
			s.res[item.TableID()] = append(s.res[item.TableID()], item)
		}
	}
	return nil
}

func SniffSnapshotData(ctx context.Context, tr *abstract.TestResult, transfer *model.Transfer) *abstract.TestResult {
	tr.Add(LoadTablesCheckType, EstimateTalbeCheckType)
	metrics := solomon.NewRegistry(solomon.NewRegistryOpts())
	tables, err := ObtainAllSrcTables(transfer, metrics)
	if err != nil {
		return tr.NotOk(LoadTablesCheckType, errors.CategorizedErrorf(categories.Source, "unable to load table list: %w", err))
	}
	tr.Ok(LoadTablesCheckType)

	sourceStorage, err := storage.NewStorage(transfer, coordinator.NewFakeClient(), metrics)
	if err != nil {
		return tr.NotOk(ConfigCheckType, errors.CategorizedErrorf(categories.Source, ResolveStorageErrorText, err))
	}
	tr.Ok(ConfigCheckType)
	tr.Preview = map[abstract.TableID][]abstract.ChangeItem{}
	defer sourceStorage.Close()
	var errs util.Errors
	previewSchemas := abstract.TableMap{}
	for table := range tables {
		cctx, cancel := context.WithTimeout(ctx, time.Minute)
		collector := &sampleCollector{
			res:    map[abstract.TableID][]abstract.ChangeItem{},
			ctx:    cctx,
			cancel: cancel,
		}
		wrapper, err := middlewares.Transformation(transfer, logger.Log, metrics)
		if err != nil {
			return tr.NotOk(ConfigCheckType, xerrors.Errorf("unable to assign transformer: %w", err))
		}
		sinker := wrapper(collector)
		if abstract.IsSystemTable(table.Name) {
			continue
		}
		tdesc := abstract.TableDescription{
			Name:   table.Name,
			Schema: table.Namespace,
			Filter: "",
			EtaRow: 0,
			Offset: 0,
		}

		cnt, err := sourceStorage.EstimateTableRowsCount(table)
		if err != nil {
			exactCnt, exactErr := sourceStorage.ExactTableRowsCount(table)
			if exactErr != nil {
				return tr.NotOk(EstimateTalbeCheckType, xerrors.Errorf("unable to extract table rows count: estimate count: %w\nexact count: %w", err, exactErr))
			}
			cnt = exactCnt
		}

		if sampleable, ok := sourceStorage.(abstract.SampleableStorage); ok && cnt > 2000 {
			err = sampleable.LoadRandomSample(tdesc, sinker.Push)
		} else {
			err = sourceStorage.LoadTable(cctx, tdesc, sinker.Push)
		}

		if err != nil && len(collector.res) == 0 {
			logger.Log.Warnf("unable to load: %v: %v", table, err)
			categorizedErr := errors.ToTransferStatusMessage(err)
			errs = append(errs, xerrors.Errorf("failed to load: %s: %s: %s", tdesc.String(), categorizedErr.Categories, categorizedErr.Heading))
		}
		for tid, items := range collector.res {
			if len(items) == 0 {
				schema, err := sourceStorage.TableSchema(ctx, tid)
				if err != nil {
					return tr.NotOk(LoadSampleCheckType, xerrors.Errorf("unable to load: %s schema: %w", tid.Fqtn(), err))
				}
				previewSchemas[tid] = abstract.TableInfo{
					EtaRow: 0,
					IsView: false,
					Schema: schema,
				}
				continue
			}
			previewSchemas[tid] = abstract.TableInfo{
				EtaRow: 0,
				IsView: false,
				Schema: items[0].TableSchema,
			}
			if len(items) > 3 {
				items = items[:3]
			}
			tr.Preview[tid] = items
		}
	}
	tr.Schema = previewSchemas

	if len(errs) > 0 {
		return tr.NotOk(LoadSampleCheckType, xerrors.Errorf("found failures %v: %w", len(errs), errs))
	}

	tr.Ok(EstimateTalbeCheckType)
	tr.Ok(LoadSampleCheckType)
	return tr
}

func TestEndpoint(ctx context.Context, param *TestEndpointParams, tr *abstract.TestResult) *abstract.TestResult {
	if !param.IsSource {
		tr.Add(ConfigCheckType, WriteableCheckType)
		dst, err := model.NewDestination(param.Type, param.Params)
		if err != nil {
			return tr.NotOk(ConfigCheckType, errors.CategorizedErrorf(categories.Target, "unable to construct target: %w", err))
		}
		tr.Ok(ConfigCheckType)
		param.Transfer.Dst = dst
		param.Transfer.Src = new(model.MockSource)
		if err := TestTargetEndpoint(param.Transfer); err != nil {
			return tr.NotOk(WriteableCheckType, errors.CategorizedErrorf(categories.Target, "unable to test target endpoint: %w", err))
		}
		tr.Ok(WriteableCheckType)
		return tr
	}
	tr.Add(ConfigCheckType)
	param.Transfer.Dst = new(model.MockDestination)
	endpointSource, err := model.NewSource(param.Type, param.Params)
	if err != nil {
		return tr.NotOk(ConfigCheckType, errors.CategorizedErrorf(categories.Source, "unable to get endpoint endpointSource: %w", err))
	}
	param.Transfer.Src = endpointSource
	if len(param.TransformationConfig) > 0 {
		if err := param.Transfer.TransformationFromJSON(string(param.TransformationConfig)); err != nil {
			return tr.NotOk(ConfigCheckType, errors.CategorizedErrorf(categories.Internal, "unable to restore transform config: %w", err))
		}
	}
	if tester, ok := providers.Source[providers.Tester](
		logger.Log,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
		coordinator.NewFakeClient(),
		param.Transfer,
	); ok {
		tr = tester.Test(ctx)
		if tr.Err() != nil {
			return tr
		}
	}

	if _, ok := providers.Source[providers.Snapshot](
		logger.Log,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
		coordinator.NewFakeClient(),
		param.Transfer,
	); ok {
		return SniffSnapshotData(ctx, tr, param.Transfer)
	}

	tr.Ok(ConfigCheckType)

	if snifferP, ok := providers.Source[providers.Sniffer](
		logger.Log,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
		coordinator.NewFakeClient(),
		param.Transfer,
	); ok {
		tr.Add(CredentialsCheckType, DataSampleCheckType, ConfigCheckType)
		sniffer, err := snifferP.Sniffer(ctx)
		if err != nil {
			return tr.NotOk(ConfigCheckType, errors.CategorizedErrorf(categories.Source, "unable to construct sniffer: %w", err))
		}
		tr.Ok(CredentialsCheckType)
		return SniffReplicationData(ctx, sniffer, tr, param.Transfer)
	}
	return tr
}

func TestTargetEndpoint(transfer *model.Transfer) error {
	switch dst := transfer.Dst.(type) {
	case *postgres.PgDestination:
		// _ping and other tables created if MaintainTables is set to true
		dstMaintainTables := dst.MaintainTables
		dst.MaintainTables = true

		// restoring destination's MaintainTables value
		defer func() {
			dst.MaintainTables = dstMaintainTables
		}()
	}
	sink, err := sink.MakeAsyncSink(transfer, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), coordinator.NewFakeClient(), middlewares.MakeConfig(middlewares.WithNoData))
	if err != nil {
		return xerrors.Errorf("unable to make sinker: %w", err)
	}
	defer sink.Close()
	return pingSinker(sink)
}
