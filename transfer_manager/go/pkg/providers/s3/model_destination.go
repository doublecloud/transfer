package s3

import (
	"encoding/gob"
	"time"

	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/middlewares/async/bufferer"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/clickhouse/model"
)

func init() {
	gob.RegisterName("*server.S3Destination", new(S3Destination))
	server.RegisterDestination(ProviderType, func() server.Destination {
		return new(S3Destination)
	})
	abstract.RegisterProviderName(ProviderType, "ObjectStorage")
}

const (
	ProviderType = abstract.ProviderType("s3")
)

type Encoding string

const (
	NoEncoding   = Encoding("UNCOMPRESSED")
	GzipEncoding = Encoding("GZIP")
)

type S3Destination struct {
	OutputFormat     server.ParsingFormat
	OutputEncoding   Encoding
	BufferSize       server.BytesSize
	BufferInterval   time.Duration
	Endpoint         string
	Region           string
	AccessKey        string
	S3ForcePathStyle bool
	Secret           string
	ServiceAccountID string
	Layout           string
	LayoutTZ         string
	LayoutColumn     string
	Bucket           string
	UseSSL           bool
	VerifySSL        bool
	PartSize         int64
	Concurrency      int64
	AnyAsString      bool
}

var _ server.Destination = (*S3Destination)(nil)

func (d *S3Destination) WithDefaults() {
	if d.Layout == "" {
		d.Layout = "2006/01/02"
	}
	if d.BufferInterval == 0 {
		d.BufferInterval = time.Second * 30
	}
	if d.BufferSize == 0 {
		d.BufferSize = server.BytesSize(model.BufferTriggingSizeDefault)
	}
	if d.Concurrency == 0 {
		d.Concurrency = 4
	}
}

func (d *S3Destination) BuffererConfig() bufferer.BuffererConfig {
	return bufferer.BuffererConfig{
		TriggingCount:    0,
		TriggingSize:     uint64(d.BufferSize),
		TriggingInterval: d.BufferInterval,
	}
}

func (d *S3Destination) ConnectionConfig() ConnectionConfig {
	return ConnectionConfig{
		AccessKey:        d.AccessKey,
		S3ForcePathStyle: d.S3ForcePathStyle,
		SecretKey:        server.SecretString(d.Secret),
		Endpoint:         d.Endpoint,
		UseSSL:           d.UseSSL,
		VerifySSL:        d.VerifySSL,
		Region:           d.Region,
		ServiceAccountID: d.ServiceAccountID,
	}
}

func (d *S3Destination) Transformer() map[string]string {
	return map[string]string{}
}

func (d *S3Destination) CleanupMode() server.CleanupType {
	return server.DisabledCleanup
}

func (S3Destination) IsDestination() {
}

func (d *S3Destination) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (d *S3Destination) Validate() error {
	return nil
}

func (d *S3Destination) compatible(src server.Source) bool {
	parseable, ok := src.(server.Parseable)
	if d.OutputFormat == server.ParsingFormatRaw {
		if ok {
			return parseable.Parser() == nil
		}
		return false
	} else {
		if ok {
			return parseable.Parser() != nil
		}
		return true
	}
}

func (d *S3Destination) Compatible(src server.Source, _ abstract.TransferType) error {
	if d.compatible(src) {
		return nil
	}
	return xerrors.Errorf("object storage %s format not compatible", d.OutputFormat)
}
