package middlewares

import (
	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
)

// PluggableTransformer is a transformer with a middleware interface which packages outside of `middlewares` can provide.
type PluggableTransformer func(*server.Transfer, metrics.Registry, coordinator.Coordinator) func(abstract.Sinker) abstract.Sinker

var chain PluggableTransformer = func(t *server.Transfer, r metrics.Registry, cp coordinator.Coordinator) func(abstract.Sinker) abstract.Sinker {
	return func(s abstract.Sinker) abstract.Sinker {
		return s
	}
}

// PlugTransformer adds a new pluggable transformer to a chain of such transformers.
// This method should be called from `init()` function.
func PlugTransformer(pt PluggableTransformer) {
	oldChain := chain
	chain = func(t *server.Transfer, r metrics.Registry, cp coordinator.Coordinator) func(abstract.Sinker) abstract.Sinker {
		return func(s abstract.Sinker) abstract.Sinker {
			return pt(t, r, cp)(oldChain(t, r, cp)(s))
		}
	}
}

func PluggableTransformersChain(t *server.Transfer, r metrics.Registry, cp coordinator.Coordinator) func(abstract.Sinker) abstract.Sinker {
	return chain(t, r, cp)
}
