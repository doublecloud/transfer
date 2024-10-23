package v1

import (
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TransferSpec defines the desired state of Transfer
type TransferSpec struct {
	Description       string                `json:"description,omitempty"`
	Type              abstract.TransferType `json:"type"`
	Src               Endpoint              `json:"src"`
	Dst               Endpoint              `json:"dst"`
	Incremental       []IncrementalTable    `json:"incremental,omitempty"`
	RegularSnapshot   *RegularSnapshot      `json:"regular_snapshot,omitempty"`
	Transformation    string                `json:"transformation,omitempty"`
	DataObjects       []string              `json:"data_objects,omitempty"`
	TypeSystemVersion int                   `json:"type_system_version,omitempty"`
	Env               map[string]string     `json:"env,omitempty"`
}

type RegularSnapshot struct {
	Enabled               bool   `json:"enabled"`
	CronExpression        string `json:"cron_expression"`
	IncrementDelaySeconds int64  `json:"increment_delay_seconds"`
}

type IncrementalTable struct {
	Name         string `json:"name,omitempty"`
	Namespace    string `json:"namespace,omitempty"`
	CursorField  string `json:"cursor_field,omitempty"`
	InitialState string `json:"initial_state,omitempty"`
}

type Endpoint struct {
	Name   string                `json:"name,omitempty"`
	Type   abstract.ProviderType `json:"type,omitempty"`
	Params string                `json:"params,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster

// Transfer is the Schema for the transfers API
type Transfer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec      TransferSpec                `json:"spec,omitempty"`
	Status    TransferStatus              `json:"status,omitempty"`
	Config    TransferConfig              `json:"config"`
	Resources corev1.ResourceRequirements `json:"resources"`
}

type TransferStatus struct {
	Status           model.TransferStatus `json:"status,omitempty"`
	SnapshotComplete bool                 `json:"snapshot_complete"`
}

type TransferConfig struct {
	Image       string            `json:"image"`
	Coordinator CoordinatorConfig `json:"coordinator"`
}

type CoordinatorConfig struct {
	S3Bucket string `json:"s3_bucket"`
	Type     string `json:"type"`
}

// +kubebuilder:object:root=true

// TransferList contains a list of Transfer
type TransferList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Transfer `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Transfer{}, &TransferList{})
}
