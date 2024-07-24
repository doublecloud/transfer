package model

type DataObjects struct {
	IncludeObjects []string `json:"includeObjects" yaml:"include_objects"`
}

func (o *DataObjects) GetIncludeObjects() []string {
	if o == nil {
		return nil
	}
	return o.IncludeObjects
}
