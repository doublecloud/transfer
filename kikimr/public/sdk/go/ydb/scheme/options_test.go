package scheme

import (
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
)

func TestSchemeOptions(t *testing.T) {
	{
		opts := []PermissionsOption{
			WithClearPermissions(),
			WithChangeOwner("ow"),
			WithGrantPermissions(Permissions{
				Subject:         "grant",
				PermissionNames: []string{"a", "b", "c"},
			}),
			WithSetPermissions(Permissions{
				Subject:         "set",
				PermissionNames: []string{"d"},
			}),
			WithRevokePermissions(Permissions{
				Subject:         "revoke",
				PermissionNames: []string{"e"},
			}),
		}

		var desc permissionsDesc
		for _, opt := range opts {
			opt(&desc)
		}

		if !desc.clear {
			t.Errorf("Clear is not as expected")
		}

		count := len(desc.actions)
		for _, a := range desc.actions {
			switch a := a.Action.(type) {
			case *Ydb_Scheme.PermissionsAction_ChangeOwner:
				count -= 1
				if a.ChangeOwner != "ow" {
					t.Errorf("Owner is not as expected")
				}
			case *Ydb_Scheme.PermissionsAction_Grant:
				count -= 1
				if a.Grant.Subject != "grant" || len(a.Grant.PermissionNames) != 3 {
					t.Errorf("Grant is not as expected")
				}
			case *Ydb_Scheme.PermissionsAction_Set:
				count -= 1
				if a.Set.Subject != "set" || len(a.Set.PermissionNames) != 1 || a.Set.PermissionNames[0] != "d" {
					t.Errorf("Set is not as expected")
				}
			case *Ydb_Scheme.PermissionsAction_Revoke:
				count -= 1
				if a.Revoke.Subject != "revoke" || len(a.Revoke.PermissionNames) != 1 || a.Revoke.PermissionNames[0] != "e" {
					t.Errorf("Revoke is not as expected")
				}
			}
		}

		if count != 0 {
			t.Errorf("Count of permission actions is not as expected")
		}
	}
}
