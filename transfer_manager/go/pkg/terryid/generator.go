package terryid

import (
	"github.com/doublecloud/tross/transfer_manager/go/pkg/randutil"
)

const (
	cloudIDGenPartLen = 17
	cloudValidIDRunes = "abcdefghijklmnopqrstuv0123456789"

	endpointPrefix      = "dte"
	transferPrefix      = "dtt"
	jobPrefix           = "dtj"
	statusMessagePrefix = "tsm"
	transformerPrefix   = "dtr"
)

func GenerateSuffix() string {
	return randutil.GenerateString(cloudValidIDRunes, cloudIDGenPartLen)
}

func GenerateTransferID() string {
	suffix := GenerateSuffix()
	return transferPrefix + suffix
}

func GenerateEndpointID() string {
	suffix := GenerateSuffix()
	return endpointPrefix + suffix
}

func GenerateJobID() string {
	suffix := GenerateSuffix()
	return jobPrefix + suffix
}

func GenerateTransferStatusMessageID() string {
	return statusMessagePrefix + GenerateSuffix()
}

func GenerateTransformerID() string {
	return transformerPrefix + GenerateSuffix()
}
