package action

type Format struct {
	Provider string            `json:"provider,omitempty"`
	Options  map[string]string `json:"options,omitempty"`
}
