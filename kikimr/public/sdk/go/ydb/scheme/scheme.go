package scheme

import (
	"context"

	ydb "github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/internal"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
)

type EntryType uint

const (
	EntryTypeUnknown EntryType = iota
	EntryDirectory
	EntryTable
	EntryPersQueueGroup
	EntryDatabase
	EntryRtmrVolume
	EntryBlockStoreVolume
	EntryCoordinationNode
	EntryColumnTable
	EntryColumnStore
)

func (t EntryType) String() string {
	switch t {
	default:
		return "Unknown"
	case EntryDirectory:
		return "Directory"
	case EntryTable:
		return "Table"
	case EntryPersQueueGroup:
		return "PersQueueGroup"
	case EntryDatabase:
		return "Database"
	case EntryRtmrVolume:
		return "RtmrVolume"
	case EntryBlockStoreVolume:
		return "BlockStoreVolume"
	case EntryCoordinationNode:
		return "CoordinationNode"
	case EntryColumnTable:
		return "ColumnTable"
	case EntryColumnStore:
		return "ColumnStore"
	}
}

type Permissions struct {
	Subject         string
	PermissionNames []string
}

type Entry struct {
	Name                 string
	Owner                string
	Type                 EntryType
	Permissions          []Permissions
	EffectivePermissions []Permissions
}

type Directory struct {
	Entry
	Children []Entry
}

type Client struct {
	Driver ydb.Driver
}

func (c *Client) MakeDirectory(ctx context.Context, path string) (err error) {
	req := Ydb_Scheme.MakeDirectoryRequest{
		Path: path,
	}
	_, err = c.Driver.Call(ctx, internal.Wrap("/Ydb.Scheme.V1.SchemeService/MakeDirectory", &req, nil))
	return
}

func (c *Client) RemoveDirectory(ctx context.Context, path string) (err error) {
	req := Ydb_Scheme.RemoveDirectoryRequest{
		Path: path,
	}
	_, err = c.Driver.Call(ctx, internal.Wrap(
		"/Ydb.Scheme.V1.SchemeService/RemoveDirectory", &req, nil,
	))
	return
}

func (c *Client) ListDirectory(ctx context.Context, path string) (d Directory, err error) {
	var res Ydb_Scheme.ListDirectoryResult
	req := Ydb_Scheme.ListDirectoryRequest{
		Path: path,
	}
	_, err = c.Driver.Call(ctx, internal.Wrap(
		"/Ydb.Scheme.V1.SchemeService/ListDirectory", &req, &res,
	))
	if err != nil {
		return d, err
	}
	d.Entry.from(res.Self)
	d.Children = make([]Entry, len(res.Children))
	putEntry(d.Children, res.Children)
	return d, nil
}

func (c *Client) DescribePath(ctx context.Context, path string) (e Entry, err error) {
	var res Ydb_Scheme.DescribePathResult
	req := Ydb_Scheme.DescribePathRequest{
		Path: path,
	}
	_, err = c.Driver.Call(ctx, internal.Wrap(
		"/Ydb.Scheme.V1.SchemeService/DescribePath", &req, &res,
	))
	if err != nil {
		return e, err
	}
	e.from(res.Self)
	return
}

func (c *Client) ModifyPermissions(ctx context.Context, path string, opts ...PermissionsOption) (err error) {
	var desc permissionsDesc
	for _, opt := range opts {
		opt(&desc)
	}
	req := Ydb_Scheme.ModifyPermissionsRequest{
		Path:             path,
		Actions:          desc.actions,
		ClearPermissions: desc.clear,
	}
	_, err = c.Driver.Call(ctx, internal.Wrap(
		"/Ydb.Scheme.V1.SchemeService/ModifyPermissions", &req, nil,
	))
	return
}

func (e *Entry) from(y *Ydb_Scheme.Entry) {
	var (
		n = len(y.Permissions)
		m = len(y.EffectivePermissions)
		p = make([]Permissions, n+m)
	)
	putPermissions(p[:n], y.Permissions)
	putPermissions(p[n:], y.EffectivePermissions)
	*e = Entry{
		Name:                 y.Name,
		Owner:                y.Owner,
		Type:                 entryType(y.Type),
		Permissions:          p[0:n],
		EffectivePermissions: p[n : n+m],
	}
}

func entryType(t Ydb_Scheme.Entry_Type) EntryType {
	switch t {
	case Ydb_Scheme.Entry_DIRECTORY:
		return EntryDirectory
	case Ydb_Scheme.Entry_TABLE:
		return EntryTable
	case Ydb_Scheme.Entry_PERS_QUEUE_GROUP:
		return EntryPersQueueGroup
	case Ydb_Scheme.Entry_DATABASE:
		return EntryDatabase
	case Ydb_Scheme.Entry_RTMR_VOLUME:
		return EntryRtmrVolume
	case Ydb_Scheme.Entry_BLOCK_STORE_VOLUME:
		return EntryBlockStoreVolume
	case Ydb_Scheme.Entry_COORDINATION_NODE:
		return EntryCoordinationNode
	case Ydb_Scheme.Entry_COLUMN_STORE:
		return EntryColumnStore
	case Ydb_Scheme.Entry_COLUMN_TABLE:
		return EntryColumnTable
	default:
		return EntryTypeUnknown
	}
}

func (p Permissions) to(y *Ydb_Scheme.Permissions) {
	y.Subject = p.Subject
	y.PermissionNames = p.PermissionNames
}

func (p *Permissions) from(y *Ydb_Scheme.Permissions) {
	*p = Permissions{
		Subject:         y.Subject,
		PermissionNames: y.PermissionNames,
	}
}

func putEntry(dst []Entry, src []*Ydb_Scheme.Entry) {
	for i, e := range src {
		(dst[i]).from(e)
	}
}

func putPermissions(dst []Permissions, src []*Ydb_Scheme.Permissions) {
	for i, p := range src {
		(dst[i]).from(p)
	}
}

func InnerConvertEntry(y *Ydb_Scheme.Entry) *Entry {
	res := &Entry{}
	res.from(y)
	return res
}
