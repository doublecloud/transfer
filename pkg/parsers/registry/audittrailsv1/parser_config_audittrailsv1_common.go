package audittrailsv1

type ParserConfigAuditTrailsV1Common struct {
	UseElasticSchema       bool // Hidden parameter only for internal usage.
	RemoveNestingInDetails bool // Convert details to map[string]string instead of map[string]any.
}

func (c *ParserConfigAuditTrailsV1Common) IsNewParserConfig() {}

func (c *ParserConfigAuditTrailsV1Common) IsAppendOnly() bool {
	return true
}

func (c *ParserConfigAuditTrailsV1Common) Validate() error {
	return nil
}
