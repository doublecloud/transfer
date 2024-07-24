package typesystem

func registerFallbackFactory(registry *[]FallbackFactory, factory FallbackFactory) {
	*registry = append(*registry, factory)
}

var SourceFallbackFactories = make([]FallbackFactory, 0)

var TargetFallbackFactories = make([]FallbackFactory, 0)
