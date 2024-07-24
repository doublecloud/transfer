package cloudevents

type ParserConfigCloudEventsCommon struct {
	SkipAuth         bool
	Username         string
	Password         string
	PasswordFallback string
	TLSFile          string
}

func (c *ParserConfigCloudEventsCommon) IsNewParserConfig() {}

func (c *ParserConfigCloudEventsCommon) IsAppendOnly() bool {
	return true
}

func (c *ParserConfigCloudEventsCommon) Validate() error {
	return nil
}
