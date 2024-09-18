package confluent

import "regexp"

var reYSR = regexp.MustCompile(`^https://.*\.schema-registry\.yandex-team\.ru$`)
var reYSRPreprod = regexp.MustCompile(`^https://.*\.schema-registry-preprod\.yandex-team\.ru$`)

func isYSRHost(in string) bool {
	return reYSR.MatchString(in) || reYSRPreprod.MatchString(in)
}
