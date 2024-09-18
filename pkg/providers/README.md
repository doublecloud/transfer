## Data Plane Providers

Each integrated data storage is combination of provider interface.
**Provider** is a struct that combine enabled features of storage that this provider implements.
Bare minimum implementation for providers looks follow:

```go
// Register provider factory into data plane provider registry
func init() {
	providers.Register(ProviderType, New)
}

// Define new ProviderType enum value
const ProviderType = abstract.ProviderType("my-awesome-provider")

// Provider impl
type MyAwesomeProvider struct {
	logger   log.Logger
	registry metrics.Registry
	cp       cpclient.ControlPlane
	transfer *server.Transfer
}

// Define type for new provider Imple
func (p MyAwesomeProvider) Type() abstract.ProviderType {
	return ProviderType
}

func New(lgr log.Logger, registry metrics.Registry, cp cpclient.ControlPlane, transfer *server.Transfer) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
```

### Depenency graph

Provider is one of core interfaces. Each specific provider register itself in providers registry.
Each task/sink/storage construct anything provider-related from providers registry.

![new-dep-graph](https://jing.yandex-team.ru/files/tserakhau/providers.drawio.svg)
