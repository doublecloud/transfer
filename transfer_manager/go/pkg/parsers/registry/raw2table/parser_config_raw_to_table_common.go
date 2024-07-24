package raw2table

type ParserConfigRawToTableCommon struct {
	IsAddTimestamp bool
	IsAddHeaders   bool
	IsAddKey       bool
	IsKeyString    bool
	IsValueString  bool
	IsTopicAsName  bool
	TableName      string
}

func (c *ParserConfigRawToTableCommon) IsNewParserConfig() {}

func (c *ParserConfigRawToTableCommon) IsAppendOnly() bool {
	return true
}

func (c *ParserConfigRawToTableCommon) Validate() error {
	return nil
}
