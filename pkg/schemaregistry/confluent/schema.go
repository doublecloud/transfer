package confluent

type Schema struct {
	ID         int    // global schema ID, -1 if unknown (in references, for example)
	Schema     string // body
	SchemaType SchemaType
	References []SchemaReference
}
