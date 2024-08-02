package yav

type CreateVersionRequest struct {
	Comment string  `json:"comment,omitempty"`
	Values  []Value `json:"value,omitempty"`
}

type CreateVersionFromDiffRequest struct {
	RequireHead bool    `json:"check_head,omitempty"`
	Diff        []Value `json:"diff,omitempty"`
	Comment     string  `json:"comment,omitempty"`
	TTL         uint64  `json:"ttl,omitempty"`
}
