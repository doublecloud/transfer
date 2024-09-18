package yt

import "os"

const (
	dataplaneVersionEnv = "DT_DP_VERSION"
)

func DataplaneVersion() (string, bool) {
	if exeVersion != "" {
		return exeVersion, true
	}
	return os.LookupEnv(dataplaneVersionEnv)
}
