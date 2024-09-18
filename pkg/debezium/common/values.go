package common

type Values struct {
	ConnectorParameters map[string]string
	V                   map[string]interface{}
}

func (v *Values) AddVal(colName string, colVal interface{}) {
	v.V[colName] = colVal
}

func NewValues(connectorParameters map[string]string) *Values {
	return &Values{
		ConnectorParameters: connectorParameters,
		V:                   make(map[string]interface{}),
	}
}
