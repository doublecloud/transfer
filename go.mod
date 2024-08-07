module github.com/doublecloud/tross

go 1.22.0

require (
	cloud.google.com/go v0.112.1
	cuelang.org/go v0.4.3
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/DataDog/zstd v1.5.2
	github.com/OneOfOne/xxhash v1.2.8
	github.com/andybalholm/brotli v1.1.0
	github.com/brianvoe/gofakeit/v6 v6.22.0
	github.com/cenkalti/backoff/v4 v4.2.1
	github.com/cloudevents/sdk-go/binding/format/protobuf/v2 v2.15.0
	github.com/docker/go-connections v0.5.0
	github.com/dustin/go-humanize v1.0.1
	github.com/go-sql-driver/mysql v1.7.1
	github.com/gofrs/uuid v4.2.0+incompatible
	github.com/golang-jwt/jwt/v4 v4.5.0
	github.com/golang/mock v1.7.0-rc.1
	github.com/golang/protobuf v1.5.4
	github.com/golang/snappy v0.0.4
	github.com/google/go-cmp v0.6.0
	github.com/google/uuid v1.6.0
	github.com/jackc/pgconn v1.14.0
	github.com/jackc/pgproto3/v2 v2.3.2
	github.com/jackc/pgtype v1.14.0
	github.com/jackc/pgx/v4 v4.18.0
	github.com/jhump/protoreflect v1.15.6
	github.com/jmoiron/sqlx v1.3.5
	github.com/mattn/go-isatty v0.0.20
	github.com/montanaflynn/stats v0.7.0
	github.com/olekukonko/tablewriter v0.0.5
	github.com/pion/logging v0.2.2
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.18.0
	github.com/prometheus/client_model v0.5.0
	github.com/prometheus/common v0.46.0
	github.com/segmentio/kafka-go v0.4.47
	github.com/shirou/gopsutil/v3 v3.24.2
	github.com/shopspring/decimal v1.3.1
	github.com/siddontang/go-log v0.0.0-20190221022429-1e957dd83bed
	github.com/sirupsen/logrus v1.9.3
	github.com/spf13/cast v1.6.0
	github.com/spf13/cobra v1.8.0
	github.com/stretchr/testify v1.9.0
	github.com/testcontainers/testcontainers-go v0.31.0
	github.com/valyala/fastjson v1.6.4
	github.com/ydb-platform/ydb-go-genproto v0.0.0-20240528144234-5d5a685e41f7
	github.com/ydb-platform/ydb-go-sdk/v3 v3.76.1
	go.uber.org/atomic v1.11.0
	go.uber.org/goleak v1.3.0
	go.uber.org/mock v0.4.0
	go.uber.org/zap v1.27.0
	golang.org/x/exp v0.0.0-20240222234643-814bf88cf225
	golang.org/x/sync v0.7.0
	golang.org/x/text v0.16.0
	golang.org/x/xerrors v0.0.0-20231012003039-104605ab7028
	golang.yandex/hasql v1.1.1
	google.golang.org/genproto v0.0.0-20240221002015-b0ce06bbee7c
	google.golang.org/genproto/googleapis/api v0.0.0-20240311173647-c811ad7063a7
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240314234333-6e1732d8331c
	google.golang.org/grpc v1.62.1
	google.golang.org/protobuf v1.34.2
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/docker/docker v25.0.5+incompatible // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/mattn/go-sqlite3 v1.14.16 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/spf13/pflag v1.0.6-0.20201009195203-85dd5c8bc61c // indirect
	go.opentelemetry.io/otel v1.28.0 // indirect
	go.opentelemetry.io/otel/trace v1.28.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.24.0 // indirect
	golang.org/x/sys v0.21.0 // indirect
)

replace github.com/insomniacslk/dhcp => github.com/insomniacslk/dhcp v0.0.0-20210120172423-cc9239ac6294

replace cloud.google.com/go/pubsub => cloud.google.com/go/pubsub v1.30.0

replace google.golang.org/grpc => google.golang.org/grpc v1.56.3

replace github.com/jackc/pgtype => github.com/jackc/pgtype v1.12.0

replace github.com/aws/aws-sdk-go => github.com/aws/aws-sdk-go v1.46.7

replace k8s.io/api => k8s.io/api v0.26.1

replace k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.26.1

replace k8s.io/apimachinery => k8s.io/apimachinery v0.26.1

replace k8s.io/apiserver => k8s.io/apiserver v0.26.1

replace k8s.io/cli-runtime => k8s.io/cli-runtime v0.26.1

replace k8s.io/client-go => k8s.io/client-go v0.26.1

replace k8s.io/cloud-provider => k8s.io/cloud-provider v0.26.1

replace k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.26.1

replace k8s.io/code-generator => k8s.io/code-generator v0.26.1

replace k8s.io/component-base => k8s.io/component-base v0.26.1

replace k8s.io/cri-api => k8s.io/cri-api v0.23.5

replace k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.26.1

replace k8s.io/dynamic-resource-allocation => k8s.io/dynamic-resource-allocation v0.26.1

replace k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.26.1

replace k8s.io/kube-proxy => k8s.io/kube-proxy v0.26.1

replace k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.26.1

replace k8s.io/kubelet => k8s.io/kubelet v0.26.1

replace k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.26.1

replace k8s.io/mount-utils => k8s.io/mount-utils v0.26.2-rc.0

replace k8s.io/pod-security-admission => k8s.io/pod-security-admission v0.26.1

replace k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.26.1

replace github.com/temporalio/features => github.com/temporalio/features v0.0.0-20231218231852-27c681667dae

replace github.com/temporalio/features/features => github.com/temporalio/features/features v0.0.0-20231218231852-27c681667dae

replace github.com/temporalio/features/harness/go => github.com/temporalio/features/harness/go v0.0.0-20231218231852-27c681667dae

replace github.com/temporalio/omes => github.com/temporalio/omes v0.0.0-20240429210145-5fa5c107b7a8

replace github.com/goccy/go-yaml => github.com/goccy/go-yaml v1.9.5

replace github.com/aleroyer/rsyslog_exporter => github.com/prometheus-community/rsyslog_exporter v1.1.0
