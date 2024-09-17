package tcrecipes

import "os"

func Enabled() bool {
	return os.Getenv("USE_TESTCONTAINERS") == "1"
}
