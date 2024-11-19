package audittrailsv1

type ParserConfigAuditTrailsV1Common struct {
	FieldsToRemoveNesting []string // Convert specified fields to map[string]string instead of map[string]any.
	UseElasticSchema      bool     // Hidden parameter only for internal usage.
}

func (c *ParserConfigAuditTrailsV1Common) IsNewParserConfig() {}

func (c *ParserConfigAuditTrailsV1Common) IsAppendOnly() bool {
	return true
}

func (c *ParserConfigAuditTrailsV1Common) Validate() error {
	return nil
}
