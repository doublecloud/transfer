package confluentschemaregistry

type ParserConfigConfluentSchemaRegistryLb struct {
	SchemaRegistryURL string
	SkipAuth          bool
	Username          string
	Password          string
	TLSFile           string
}

func (c *ParserConfigConfluentSchemaRegistryLb) IsNewParserConfig() {}

func (c *ParserConfigConfluentSchemaRegistryLb) IsAppendOnly() bool {
	return true
}

func (c *ParserConfigConfluentSchemaRegistryLb) Validate() error {
	return nil
}
