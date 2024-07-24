package debezium

type ParserConfigDebeziumLb struct {
	SchemaRegistryURL string
	SkipAuth          bool
	Username          string
	Password          string
	TLSFile           string
}

func (c *ParserConfigDebeziumLb) IsNewParserConfig() {}

func (c *ParserConfigDebeziumLb) IsAppendOnly() bool {
	return false
}

func (c *ParserConfigDebeziumLb) Validate() error {
	return nil
}
