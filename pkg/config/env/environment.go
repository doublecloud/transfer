package env

type Environment int

const (
	EnvironmentUnknown  Environment = 0
	EnvironmentInternal Environment = 1
)

type EnvironmentProvider interface {
	Get() Environment
	Set(env Environment)
}

type TestEnvironmentProvider struct {
	env Environment
}

func NewTestEnvironmentProvider() *TestEnvironmentProvider {
	return &TestEnvironmentProvider{
		env: EnvironmentInternal,
	}
}

func (provider *TestEnvironmentProvider) Get() Environment {
	return provider.env
}

func (provider *TestEnvironmentProvider) Set(env Environment) {
	provider.env = env
}
