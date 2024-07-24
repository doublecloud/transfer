package engine

import "time"

type cloudEventsProtoFields struct {
	id         string
	source     string
	type_      string
	dataschema string
	subject    string
	time       time.Time
}
