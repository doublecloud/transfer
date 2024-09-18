package debezium

type ParserConfigDebeziumCommon struct {
	SchemaRegistryURL string
	SkipAuth          bool
	Username          string
	Password          string
	TLSFile           string
}

func (c *ParserConfigDebeziumCommon) IsNewParserConfig() {}

func (c *ParserConfigDebeziumCommon) IsAppendOnly() bool {
	return false
}

func (c *ParserConfigDebeziumCommon) Validate() error {
	return nil
}
