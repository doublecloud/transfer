package audittrailsv1

type ParserConfigAuditTrailsV1Common struct {
	UseElasticSchema bool // UI can't set this parameter - it's only for internal use - from FillDependentFields
}

func (c *ParserConfigAuditTrailsV1Common) IsNewParserConfig() {}

func (c *ParserConfigAuditTrailsV1Common) IsAppendOnly() bool {
	return true
}

func (c *ParserConfigAuditTrailsV1Common) Validate() error {
	return nil
}
