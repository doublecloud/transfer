package abstract

type ProviderType string

const (
	ProviderTypeMock = ProviderType("mock")
	ProviderTypeNone = ProviderType("none")
)

var providerName = map[ProviderType]string{
	ProviderTypeMock: "Mock",
	ProviderTypeNone: "None",
}

func RegisterProviderName(providerType ProviderType, name string) {
	providerName[providerType] = name
}

func (p ProviderType) Name() string {
	name, ok := providerName[p]
	if ok {
		return name
	}
	return string(p)
}
