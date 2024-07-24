package cloudevents

type ParserConfigCloudEventsLb struct {
	SkipAuth         bool
	Username         string
	Password         string
	PasswordFallback string
	TLSFile          string
}

func (c *ParserConfigCloudEventsLb) IsNewParserConfig() {}

func (c *ParserConfigCloudEventsLb) IsAppendOnly() bool {
	return true
}

func (c *ParserConfigCloudEventsLb) Validate() error {
	return nil
}
