package confluentschemaregistry

type ParserConfigConfluentSchemaRegistryCommon struct {
	SchemaRegistryURL string
	SkipAuth          bool
	Username          string
	Password          string
	TLSFile           string
}

func (c *ParserConfigConfluentSchemaRegistryCommon) IsNewParserConfig() {}

func (c *ParserConfigConfluentSchemaRegistryCommon) IsAppendOnly() bool {
	return true
}

func (c *ParserConfigConfluentSchemaRegistryCommon) Validate() error {
	return nil
}
