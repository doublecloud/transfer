package yav

const (
	SecretStateNormal = "normal"
	SecretStateHidden = "hidden"
)

type SecretRequest struct {
	Name    string   `json:"name,omitempty"`
	Comment string   `json:"comment,omitempty"`
	State   string   `json:"state,omitempty"`
	Tags    []string `json:"tags,omitempty"`
}

const (
	SecretOrderByUUID      = "uuid"
	SecretOrderByName      = "name"
	SecretOrderByComment   = "comment"
	SecretOrderByCreatedAt = "created_at"
	SecretOrderByCreatedBy = "created_by"
	SecretOrderByUpdatedAt = "updated_at"
	SecretOrderByUpdatedBy = "updated_by"

	SecretQueryTypeInfix    = "infix"
	SecretQueryTypeLanguage = "language"
	SecretQueryTypeExact    = "exact"
	SecretQueryTypePrefix   = "prefix"
)

type GetSecretsRequest struct {
	Asc               bool     `json:"asc,omitempty"`
	OrderBy           string   `json:"order_by,omitempty"`
	Page              uint     `json:"page,omitempty"`
	PageSize          uint     `json:"page_size,omitempty"`
	Query             string   `json:"query,omitempty"`
	QueryType         string   `json:"query_type,omitempty"`
	Role              string   `json:"role,omitempty"`
	Tags              []string `json:"tags,omitempty"`
	WithHiddenSecrets bool     `json:"with_hidden_secrets,omitempty"`
	WithTVMApps       bool     `json:"with_tvm_apps,omitempty"`
	Without           []string `json:"without,omitempty"`
	Yours             bool     `json:"yours,omitempty"`
}

const (
	AbcScopeDevelopment = "development"
)

type SecretRoleRequest struct {
	AbcID     uint   `json:"abc_id,omitempty"`
	AbcScope  string `json:"abc_scope,omitempty"`
	AbcRoleID uint   `json:"abc_role_id,omitempty"`
	Login     string `json:"login,omitempty"`
	Role      string `json:"role,omitempty"`
	StaffID   uint   `json:"staff_id,omitempty"`
	UID       uint64 `json:"uid,omitempty"` // passport user id (uid)
}

type TokenizedRequest struct {
	SecretUUID    string `json:"secret_uuid,omitempty"`
	SecretVersion string `json:"secret_version,omitempty"`
	ServiceTicket string `json:"service_ticket,omitempty"`
	Signature     string `json:"signature,omitempty"`
	Token         string `json:"token,omitempty"`
	UID           string `json:"uid,omitempty"`
}

type GetSecretVersionsByTokensRequest struct {
	TokenizedRequests []TokenizedRequest `json:"tokenized_requests,omitempty"`
}
