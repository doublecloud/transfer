package buildinfo

import (
	"github.com/doublecloud/transfer/library/go/core/buildinfo"
)

func ArcadiaVersion() string {
	if buildinfo.Info.ArcadiaSourceRevision != "-1" {
		return buildinfo.Info.ArcadiaSourceRevision
	}
	return buildinfo.Info.Hash
}
