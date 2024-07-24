package scheme

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"

type permissionsDesc struct {
	clear   bool
	actions []*Ydb_Scheme.PermissionsAction
}

func permissions(p Permissions) *Ydb_Scheme.Permissions {
	var y Ydb_Scheme.Permissions
	p.to(&y)
	return &y
}

type PermissionsOption func(*permissionsDesc)

func WithClearPermissions() PermissionsOption {
	return func(p *permissionsDesc) {
		p.clear = true
	}
}
func WithGrantPermissions(p Permissions) PermissionsOption {
	return func(d *permissionsDesc) {
		d.actions = append(d.actions, &Ydb_Scheme.PermissionsAction{
			Action: &Ydb_Scheme.PermissionsAction_Grant{
				Grant: permissions(p),
			},
		})
	}
}
func WithRevokePermissions(p Permissions) PermissionsOption {
	return func(d *permissionsDesc) {
		d.actions = append(d.actions, &Ydb_Scheme.PermissionsAction{
			Action: &Ydb_Scheme.PermissionsAction_Revoke{
				Revoke: permissions(p),
			},
		})
	}
}
func WithSetPermissions(p Permissions) PermissionsOption {
	return func(d *permissionsDesc) {
		d.actions = append(d.actions, &Ydb_Scheme.PermissionsAction{
			Action: &Ydb_Scheme.PermissionsAction_Set{
				Set: permissions(p),
			},
		})
	}
}
func WithChangeOwner(owner string) PermissionsOption {
	return func(d *permissionsDesc) {
		d.actions = append(d.actions, &Ydb_Scheme.PermissionsAction{
			Action: &Ydb_Scheme.PermissionsAction_ChangeOwner{
				ChangeOwner: owner,
			},
		})
	}
}
