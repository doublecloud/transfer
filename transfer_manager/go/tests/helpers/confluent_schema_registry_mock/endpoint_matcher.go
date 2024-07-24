package confluentsrmock

import (
	"regexp"
	"strings"
)

type endpointMatcher interface {
	IsMatched(url string) bool
}

type schemasIDS struct {
}

func (s schemasIDS) IsMatched(url string) bool {
	return strings.HasPrefix(url, "/schemas/ids/")
}

type subjectsVersions struct {
}

func (s subjectsVersions) IsMatched(url string) bool {
	return strings.HasPrefix(url, "/subjects/") && strings.HasSuffix(url, "/versions")
}

type schema struct {
}

var reSchema = regexp.MustCompile(`^/subjects/(.*)/versions/([0-9]+)$`)

func (s schema) IsMatched(url string) bool {
	return reSchema.Match([]byte(url))
}
