package transformer

import (
	"encoding/gob"
	"sort"

	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type TransformerFactory func(protoConfig any, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error)

var (
	knownTransformer = map[abstract.TransformerType]TransformerFactory{}
)

func KnownTransformerNames() []string {
	var keys []string
	for k := range knownTransformer {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)
	return keys
}

func IsKnownTransformerType(t abstract.TransformerType) bool {
	_, ok := knownTransformer[t]
	return ok
}

func Register[TConfig Config](typ abstract.TransformerType, f func(cfg TConfig, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error)) {
	gob.Register(new(TConfig))
	knownTransformer[typ] = func(genericCfg any, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
		var t TConfig
		if err := util.MapFromJSON(genericCfg, &t); err != nil {
			return nil, xerrors.Errorf("unable to map generic config: %T to %T: %w", genericCfg, t, err)
		}
		return f(t, lgr, runtime)
	}
}

func New(typ abstract.TransformerType, cfg Config, lgr log.Logger, rt abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
	fac, ok := knownTransformer[typ]
	if !ok {
		return nil, xerrors.Errorf("not supported transformer %s, known: %v", typ, util.MapKeysInOrder(knownTransformer))
	}
	return fac(cfg, lgr, rt)
}
