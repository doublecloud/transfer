package logbroker

import (
	"context"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/internal/pqconfig"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/config"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/metering/writer"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
)

var writerInitTimeout = 60 * time.Second

func init() {
	config.RegisterTypeTagged((*config.WriterConfig)(nil), (*Logbroker)(nil), (*Logbroker)(nil).Type(), nil)
	writer.Register(
		new(Logbroker).Type(),
		func(schema, topic, sourceID string, cfg writer.WriterConfig) (writer.Writer, error) {
			lbConfig := cfg.(*Logbroker)
			logger.Log.Debugf("initialize lb writer for %v: %v, topic = %v, sourceID = %v", schema, lbConfig, topic, sourceID)
			pqConfig, err := newPQConfig(lbConfig, topic, sourceID)
			if err != nil {
				return nil, xerrors.Errorf("cannot create perqueue writer config: %w", err)
			}
			res := &Writer{
				writer: persqueue.NewWriter(*pqConfig),
			}

			rollbacks := util.Rollbacks{}
			defer rollbacks.Do()

			errCh := make(chan error)
			timer := time.NewTimer(writerInitTimeout)
			ctx, cancel := context.WithCancel(context.Background())
			rollbacks.Add(cancel)
			go func() {
				_, err := res.writer.Init(ctx)
				errCh <- err
			}()

			select {
			case err = <-errCh:
			case <-timer.C:
				err = xerrors.New("timeout limit exceeded")
			}
			if err != nil {
				return nil, xerrors.Errorf("cannot start writer for %v metrics: %w", schema, err)
			}
			rollbacks.Cancel()
			return res, nil
		},
	)
}

type Logbroker struct {
	Endpoint string                      `mapstructure:"endpoint"`
	Creds    config.LogbrokerCredentials `mapstructure:"creds"`
	Database string                      `mapstructure:"database"`
	TLSMode  config.TLSMode              `mapstructure:"tls_mode"`
	Port     int                         `mapstructure:"port"`
}

func (*Logbroker) IsMeteringWriter() {}
func (*Logbroker) IsTypeTagged()     {}
func (*Logbroker) Type() string      { return "logbroker" }

func newPQConfig(lbConfig *Logbroker, topic, sourceID string) (opts *persqueue.WriterOptions, err error) {
	creds, err := pqconfig.ResolveCreds(lbConfig.Creds)
	if err != nil {
		return nil, xerrors.Errorf("Cannot init persqueue writer config without credentials client: %w", err)
	}

	tlsConfig, err := pqconfig.ResolveTLS(lbConfig.TLSMode)
	if err != nil {
		return nil, xerrors.Errorf("Cannot initialize TLS config: %w", err)
	}

	return &persqueue.WriterOptions{
		Topic:          topic,
		Endpoint:       lbConfig.Endpoint,
		Database:       lbConfig.Database,
		Credentials:    creds,
		TLSConfig:      tlsConfig,
		SourceID:       []byte(sourceID),
		RetryOnFailure: true,
		MaxMemory:      50 * 1024 * 1024,
		Port:           lbConfig.Port,
	}, nil
}

type Writer struct {
	writer persqueue.Writer
}

func (w Writer) Close() error {
	return w.writer.Close()
}

func (w Writer) Write(data string) error {
	if err := w.writer.Write(
		&persqueue.WriteMessage{
			Data:            []byte(data),
			CreateTimestamp: time.Now(),
		},
	); err != nil {
		return xerrors.Errorf("unable to write: %w", err)
	}

	rsp := <-w.writer.C()
	switch m := rsp.(type) {
	case *persqueue.Issue:
		return xerrors.Errorf("got error on push metric: %w", m.Err)
	default:
		return nil
	}
}
