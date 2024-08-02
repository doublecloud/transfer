package search

import (
	"time"

	"github.com/doublecloud/tross/transfer_manager/go/pkg/auth/permissions"
)

type ResourceType string
type ServiceName string

const (
	ResourceTypeTransfer = ResourceType("transfer")
	ResourceTypeEndPoint = ResourceType("endpoint")

	ServiceNameDataTransfer = ServiceName("data-transfer")
)

type SearchResource struct {
	ResourceType     ResourceType           `json:"resource_type"`
	Timestamp        time.Time              `json:"timestamp"`
	ResourceID       string                 `json:"resource_id"`
	Name             string                 `json:"name"`
	Service          ServiceName            `json:"service"`
	Permission       permissions.Permission `json:"permission"`
	CloudID          string                 `json:"cloud_id"`
	FolderID         string                 `json:"folder_id"`
	ResourcePath     []ResourcePath         `json:"resource_path"`
	Deleted          *time.Time             `json:"deleted,omitempty"`
	ReindexTimestamp *time.Time             `json:"reindex_timestamp,omitempty"`
	Attributes       interface{}            `json:"attributes,omitempty"`
}

type ResourcePath struct {
	ResourceType ResourceType `json:"resource_type"`
	ResourceID   string       `json:"resource_id"`
}
