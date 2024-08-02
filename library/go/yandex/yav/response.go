package yav

import "errors"

const (
	// EncodingBase64 is a secret value encoding
	EncodingBase64 = "base64"
)

type Response struct {
	Status string `json:"status"`
	Code   string `json:"code"`
}

// Err returns error if any occurred in request
func (r Response) Err() error {
	if r.Status != StatusError {
		return nil
	}

	switch r.Code {
	case ErrAbc.Error():
		return ErrAbc
	case ErrAbcScopeNotFound.Error():
		return ErrAbcScopeNotFound
	case ErrAbcRoleNotFound.Error():
		return ErrAbcRoleNotFound
	case ErrAbcServiceNotFound.Error():
		return ErrAbcServiceNotFound
	case ErrAccess.Error():
		return ErrAccess
	case ErrDecryption.Error():
		return ErrDecryption
	case ErrDelegationAccess.Error():
		return ErrDelegationAccess
	case ErrEmptyDiffSecretVersion.Error():
		return ErrEmptyDiffSecretVersion
	case ErrExternalService.Error():
		return ErrExternalService
	case ErrHeadVersionNotFound.Error():
		return ErrHeadVersionNotFound
	case ErrInvalidOAuthToken.Error():
		return ErrInvalidOAuthToken
	case ErrInvalidScopes.Error():
		return ErrInvalidScopes
	case ErrInvalidToken.Error():
		return ErrInvalidToken
	case ErrLastOwner.Error():
		return ErrLastOwner
	case ErrLoginHeaderInRSASignatureRequired.Error():
		return ErrLoginHeaderInRSASignatureRequired
	case ErrNonExistentEntity.Error():
		return ErrNonExistentEntity
	case ErrUUIDCollision.Error():
		return ErrUUIDCollision
	case ErrOutdatedRSASignature.Error():
		return ErrOutdatedRSASignature
	case ErrRSASignature.Error():
		return ErrRSASignature
	case ErrSecretHasNewHeadVersion.Error():
		return ErrSecretHasNewHeadVersion
	case ErrServiceTemporaryDown.Error():
		return ErrServiceTemporaryDown
	case ErrServiceTicketParsing.Error():
		return ErrServiceTicketParsing
	case ErrServiceTicketRequired.Error():
		return ErrServiceTicketRequired
	case ErrStaff.Error():
		return ErrStaff
	case ErrStaffGroupNotFound.Error():
		return ErrStaffGroupNotFound
	case ErrTimestampHeaderInRSASignatureRequired.Error():
		return ErrTimestampHeaderInRSASignatureRequired
	case ErrTokenDoesNotMatchSecret.Error():
		return ErrTokenDoesNotMatchSecret
	case ErrTokenNotAssociatedWithTVMApp.Error():
		return ErrTokenNotAssociatedWithTVMApp
	case ErrTokenRevoked.Error():
		return ErrTokenRevoked
	case ErrTVMGrantExists.Error():
		return ErrTVMGrantExists
	case ErrTVMGrantRequired.Error():
		return ErrTVMGrantRequired
	case ErrUnallowedTVMClient.Error():
		return ErrUnallowedTVMClient
	case ErrUserAuthRequired.Error():
		return ErrUserAuthRequired
	case ErrUserNotFound.Error():
		return ErrUserNotFound
	case ErrUserTicketParsing.Error():
		return ErrUserTicketParsing
	case ErrUserTicketRequired.Error():
		return ErrUserTicketRequired
	case ErrValidation.Error():
		return ErrValidation
	case ErrZeroDefaultUID.Error():
		return ErrZeroDefaultUID
	default:
		return errors.New(r.Code)
	}
}

type CreateSecretResponse struct {
	Response

	// Not an actual RFC4122 UUID.
	SecretUUID string `json:"uuid"`
}

type GetSecretsResponse struct {
	Response

	Page     int      `json:"page"`
	PageSize int      `json:"page_size"`
	Secrets  []Secret `json:"secrets"`
}

type GetVersionResponse struct {
	Response

	Version Version `json:"version"`
}

type CreateVersionFromDiffResponse struct {
	Response

	ParentVersionUUUID string `json:"parent_version_uuid"`
	SecretUUUID        string `json:"secret_uuid"`
	Version            string `json:"version"`
	ParentDiffKeys     struct {
		Added   []string `json:"added"`
		Changed []string `json:"changed"`
		Removed []string `json:"removed"`
	} `json:"parent_diff_keys"`
}

type CheckReadAccessRightsResponse struct {
	Response

	Access     string `json:"access"`
	Name       string `json:"name"`
	SecretUUID string `json:"secret_uuid"`
	User       User   `json:"user"`
}

type SecretVersionResponse struct {
	Response

	SecretVersion string  `json:"secret_version"`
	Status        string  `json:"status"`
	Token         string  `json:"token"`
	TokenUUID     string  `json:"token_uuid"`
	Values        []Value `json:"value"`
}

type GetSecretVersionsByTokensResponse struct {
	Response

	Secrets []SecretVersionResponse `json:"secrets"`
}

type CreateVersionResponse struct {
	Response

	// Not an actual RFC4122 UUID.
	VersionUUID string `json:"secret_version"`
}

type GetTokensResponse struct {
	Response

	Tokens []Token `json:"tokens"`
}

type CreateTokenResponse struct {
	Response

	SecretUUID string `json:"secret_uuid"`
	Token      string `json:"token"`
	TokenUUID  string `json:"token_uuid"`
	TVMApp     TVMApp `json:"tvm_app"`
}

type ACL struct {
	CreatedAt    Timestamp `json:"created_at"`
	CreatedBy    uint64    `json:"created_by"`
	CreatorLogin string    `json:"creator_login"`
	Login        string    `json:"login"`
	RoleSlug     string    `json:"role_slug"`
	UID          uint64    `json:"uid"`
}

type Secret struct {
	ACLs              []ACL             `json:"acl"`
	CreatedAt         Timestamp         `json:"created_at"`
	CreatedBy         uint64            `json:"created_by"`
	CreatorLogin      string            `json:"creator_login"`
	EffectiveRole     string            `json:"effective_role"`
	LastSecretVersion LastSecretVersion `json:"last_secret_version"`
	Name              string            `json:"name"`
	SecretRoles       []SecretRole      `json:"secret_roles"`
	TokensCount       uint              `json:"tokens_count"`
	UpdatedAt         Timestamp         `json:"updated_at"`
	UpdatedBy         uint64            `json:"updated_by"`
	SecretUUID        string            `json:"uuid"`
	VersionsCount     uint              `json:"versions_count"`
}

type LastSecretVersion struct {
	VersionUUID string `json:"version"`
}

type SecretRole struct {
	AbcID        uint      `json:"abc_id"`
	AbcName      string    `json:"abc_name"`
	AbcScope     string    `json:"abc_scope"`
	AbcScopeID   uint      `json:"abc_scope_id"`
	AbcScopeName string    `json:"abc_scope_name"`
	AbcSlug      string    `json:"abc_slug"`
	AbcRoleID    uint      `json:"abc_role"`
	AbcRoleName  string    `json:"abc_role_name"`
	AbcURL       string    `json:"abc_url"`
	CreatedAt    Timestamp `json:"created_at"`
	CreatedBy    uint64    `json:"created_by"`
	CreatorLogin string    `json:"creator_login"`
	Login        string    `json:"login"`
	RoleSlug     string    `json:"role_slug"`
	UID          uint64    `json:"uid"`
	StaffID      uint      `json:"staff_id"`
	StaffName    string    `json:"staff_name"`
	StaffSlug    string    `json:"staff_slug"`
	StaffURL     string    `json:"staff_url"`
}

type Value struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	Encoding string `json:"encoding,omitempty"`
}

type Version struct {
	CreatedAt    Timestamp `json:"created_at"`
	CreatedBy    uint64    `json:"created_by"`
	CreatorLogin string    `json:"creator_login"`
	SecretName   string    `json:"secret_name"`
	SecretUUID   string    `json:"secret_uuid"`
	Values       []Value   `json:"value"`
	VersionUUID  string    `json:"version"`
}

type ABCDepartment struct {
	DisplayName string `json:"display_name"`
	ID          uint64 `json:"id"`
	UniqueName  string `json:"unique_name"`
}

type TVMApp struct {
	ABCDepartment       ABCDepartment `json:"abc_department"`
	ABCState            string        `json:"abc_state"`
	ABCStateDisplayName string        `json:"abc_state_display_name"`
	Name                string        `json:"name"`
	TVMClientID         uint64        `json:"tvm_client_id"`
}

type Token struct {
	CreatedAt    Timestamp `json:"created_at"`
	CreatedBy    uint64    `json:"created_by"`
	CreatorLogin string    `json:"creator_login"`
	SecretUUID   string    `json:"secret_uuid"`
	Signature    string    `json:"signature"`
	StateName    string    `json:"state_name"`
	TokenUUID    string    `json:"token_uuid"`
	TVMApp       TVMApp    `json:"tvm_app"`
	TVMClientID  uint64    `json:"tvm_client_id"`
}

type User struct {
	Login string `json:"login"`
	UID   uint64 `json:"uid"`
}
