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
	return environmentProvider.Get()
}

func Set(env Environment) {
	environmentProvider.Set(env)
}

func In(allowedEnvironments ...Environment) bool {
	environment := Get()
	if environment == nil {
		return false
	}
	for _, allowedEnvironment := range allowedEnvironments {
		if allowedEnvironment == environment {
			return true
		}
	}
	return false
}
