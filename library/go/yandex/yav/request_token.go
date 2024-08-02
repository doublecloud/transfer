package yav

type GetTokensRequest struct {
	Page        uint `json:"page,omitempty"`
	PageSize    uint `json:"page_size,omitempty"`
	WithRevoked bool `json:"with_revoked,omitempty"`
}

type CreateTokenRequest struct {
	Comment     string `json:"comment,omitempty"`
	Signature   string `json:"signature,omitempty"`
	TVMClientID uint64 `json:"tvm_client_id,omitempty"`
}
