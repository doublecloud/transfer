package yav

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResponse_Err(t *testing.T) {
	testCases := []struct {
		name      string
		resp      Response
		expectErr error
	}{
		{
			"status_ok",
			Response{Status: StatusOK, Code: ""},
			nil,
		},
		{
			"status_warning",
			Response{Status: StatusWarning, Code: ""},
			nil,
		},
		{
			"custom_err",
			Response{Status: StatusError, Code: "custom_test_error"},
			errors.New("custom_test_error"),
		},
		{
			"abc_error",
			Response{Status: StatusError, Code: "abc_error"},
			ErrAbc,
		},
		{
			"abc_scope_not_found",
			Response{Status: StatusError, Code: "abc_scope_not_found"},
			ErrAbcScopeNotFound,
		},
		{
			"abc_role_not_found",
			Response{Status: StatusError, Code: "abc_role_not_found"},
			ErrAbcRoleNotFound,
		},
		{
			"abc_service_not_found",
			Response{Status: StatusError, Code: "abc_service_not_found"},
			ErrAbcServiceNotFound,
		},
		{
			"access_error",
			Response{Status: StatusError, Code: "access_error"},
			ErrAccess,
		},
		{
			"decryption_error",
			Response{Status: StatusError, Code: "decryption_error"},
			ErrDecryption,
		},
		{
			"delegation_access_error",
			Response{Status: StatusError, Code: "delegation_access_error"},
			ErrDelegationAccess,
		},
		{
			"empty_diff_secret_version",
			Response{Status: StatusError, Code: "empty_diff_secret_version"},
			ErrEmptyDiffSecretVersion,
		},
		{
			"external_service_error",
			Response{Status: StatusError, Code: "external_service_error"},
			ErrExternalService,
		},
		{
			"head_version_not_found",
			Response{Status: StatusError, Code: "head_version_not_found"},
			ErrHeadVersionNotFound,
		},
		{
			"invalid_oauth_token_error",
			Response{Status: StatusError, Code: "invalid_oauth_token_error"},
			ErrInvalidOAuthToken,
		},
		{
			"invalid_scopes_error",
			Response{Status: StatusError, Code: "invalid_scopes_error"},
			ErrInvalidScopes,
		},
		{
			"invalid_token",
			Response{Status: StatusError, Code: "invalid_token"},
			ErrInvalidToken,
		},
		{
			"last_owner_error",
			Response{Status: StatusError, Code: "last_owner_error"},
			ErrLastOwner,
		},
		{
			"login_header_in_rsa_signature_required_error",
			Response{Status: StatusError, Code: "login_header_in_rsa_signature_required_error"},
			ErrLoginHeaderInRSASignatureRequired,
		},
		{
			"nonexistent_entity_error",
			Response{Status: StatusError, Code: "nonexistent_entity_error"},
			ErrNonExistentEntity,
		},
		{
			"omg_uuid_collision_error",
			Response{Status: StatusError, Code: "omg_uuid_collision_error"},
			ErrUUIDCollision,
		},
		{
			"outdated_rsa_signature_error",
			Response{Status: StatusError, Code: "outdated_rsa_signature_error"},
			ErrOutdatedRSASignature,
		},
		{
			"rsa_signature_error",
			Response{Status: StatusError, Code: "rsa_signature_error"},
			ErrRSASignature,
		},
		{
			"secret_has_new_head_version",
			Response{Status: StatusError, Code: "secret_has_new_head_version"},
			ErrSecretHasNewHeadVersion,
		},
		{
			"service_temporary_down",
			Response{Status: StatusError, Code: "service_temporary_down"},
			ErrServiceTemporaryDown,
		},
		{
			"service_ticket_parsing_error",
			Response{Status: StatusError, Code: "service_ticket_parsing_error"},
			ErrServiceTicketParsing,
		},
		{
			"service_ticket_required_error",
			Response{Status: StatusError, Code: "service_ticket_required_error"},
			ErrServiceTicketRequired,
		},
		{
			"staff_error",
			Response{Status: StatusError, Code: "staff_error"},
			ErrStaff,
		},
		{
			"staff_group_not_found",
			Response{Status: StatusError, Code: "staff_group_not_found"},
			ErrStaffGroupNotFound,
		},
		{
			"timestamp_header_in_rsa_signature_required_error",
			Response{Status: StatusError, Code: "timestamp_header_in_rsa_signature_required_error"},
			ErrTimestampHeaderInRSASignatureRequired,
		},
		{
			"token_does_not_match_secret",
			Response{Status: StatusError, Code: "token_does_not_match_secret"},
			ErrTokenDoesNotMatchSecret,
		},
		{
			"token_not_associated_with_tvm_app",
			Response{Status: StatusError, Code: "token_not_associated_with_tvm_app"},
			ErrTokenNotAssociatedWithTVMApp,
		},
		{
			"token_revoked",
			Response{Status: StatusError, Code: "token_revoked"},
			ErrTokenRevoked,
		},
		{
			"tvm_grant_exists_error",
			Response{Status: StatusError, Code: "tvm_grant_exists_error"},
			ErrTVMGrantExists,
		},
		{
			"tvm_grant_required_error",
			Response{Status: StatusError, Code: "tvm_grant_required_error"},
			ErrTVMGrantRequired,
		},
		{
			"unallowed_tvm_client_error",
			Response{Status: StatusError, Code: "unallowed_tvm_client_error"},
			ErrUnallowedTVMClient,
		},
		{
			"user_auth_required_error",
			Response{Status: StatusError, Code: "user_auth_required_error"},
			ErrUserAuthRequired,
		},
		{
			"user_not_found",
			Response{Status: StatusError, Code: "user_not_found"},
			ErrUserNotFound,
		},
		{
			"user_ticket_parsing_error",
			Response{Status: StatusError, Code: "user_ticket_parsing_error"},
			ErrUserTicketParsing,
		},
		{
			"user_ticket_required_error",
			Response{Status: StatusError, Code: "user_ticket_required_error"},
			ErrUserTicketRequired,
		},
		{
			"validation_error",
			Response{Status: StatusError, Code: "validation_error"},
			ErrValidation,
		},
		{
			"zero_default_uid_error",
			Response{Status: StatusError, Code: "zero_default_uid_error"},
			ErrZeroDefaultUID,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectErr, tc.resp.Err())
		})
	}
}
