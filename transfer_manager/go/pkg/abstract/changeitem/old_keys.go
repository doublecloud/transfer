package changeitem

type OldKeysType struct {
	KeyNames  []string      `json:"keynames,omitempty"`
	KeyTypes  []string      `json:"keytypes,omitempty"`
	KeyValues []interface{} `json:"keyvalues,omitempty"`
}

func EmptyOldKeys() OldKeysType {
	return OldKeysType{
		KeyNames:  nil,
		KeyTypes:  nil,
		KeyValues: nil,
	}
}
