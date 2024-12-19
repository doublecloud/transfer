package env

import (
	"os"
)

var (
	environmentProvider EnvironmentProvider
)

func init() {
	if IsTest() {
		environmentProvider = NewTestEnvironmentProvider()
	}
}

func IsTest() bool {
	return os.Getenv("YA_TEST_RUNNER") == "1"
}

func Get() Environment {
	if environmentProvider == nil {
		return EnvironmentUnknown
	}
	return environmentProvider.Get()
}

func Set(env Environment) {
	environmentProvider.Set(env)
}

func In(allowedEnvironments ...Environment) bool {
	environment := Get()
	for _, allowedEnvironment := range allowedEnvironments {
		if allowedEnvironment == environment {
			return true
		}
	}
	return false
}
