package confluent

type SchemaType string

const (
	PROTOBUF SchemaType = "PROTOBUF"
	AVRO     SchemaType = "AVRO"
	JSON     SchemaType = "JSON"
)

func (s SchemaType) String() string {
	return string(s)
}
