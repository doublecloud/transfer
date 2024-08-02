package search

import (
	"context"
	"encoding/json"
	"os"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/internal/pqconfig"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/config"
)

func init() {
	config.RegisterTypeTagged((*config.WriterConfig)(nil), (*SearchLogbroker)(nil), (*SearchLogbroker)(nil).Type(), nil)
}

type SearchLogbroker struct {
	Endpoint string                      `mapstructure:"endpoint"`
	Creds    config.LogbrokerCredentials `mapstructure:"creds"`
	Database string                      `mapstructure:"database"`
	Topic    string                      `mapstructure:"topic"`
	TLSMode  config.TLSMode              `mapstructure:"tls_mode"`
	SourceID string                      `mapstructure:"source_id,local_only"`
}

func (*SearchLogbroker) IsMeteringWriter() {}
func (*SearchLogbroker) IsTypeTagged()     {}
func (*SearchLogbroker) Type() string      { return "search_logbroker" }

type LogbrokerSender struct {
	writer persqueue.Writer
}

func newSearchPQConfig(lbConfig *SearchLogbroker) (opts *persqueue.WriterOptions, err error) {
	creds, err := pqconfig.ResolveCreds(lbConfig.Creds)
	if err != nil {
		return nil, xerrors.Errorf("Cannot init persqueue writer config without credentials client: %w", err)
	}

	tlsConfig, err := pqconfig.ResolveTLS(lbConfig.TLSMode)
	if err != nil {
		return nil, xerrors.Errorf("Cannot initialize TLS config: %w", err)
	}

	sourceID := lbConfig.SourceID
	if sourceID == "" {
		sourceID, _ = os.Hostname()
	}

	return &persqueue.WriterOptions{
		Topic:          lbConfig.Topic,
		Endpoint:       lbConfig.Endpoint,
		Database:       lbConfig.Database,
		Credentials:    creds,
		TLSConfig:      tlsConfig,
		SourceID:       []byte(sourceID),
		RetryOnFailure: true,
		MaxMemory:      50 * 1024 * 1024,
	}, nil
}

func newLogbrokerSender(lbConf *SearchLogbroker) (*LogbrokerSender, error) {
	conf, err := newSearchPQConfig(lbConf)
	if err != nil {
		return nil, xerrors.Errorf("error while creating pq config: %v", err)
	}

	p := persqueue.NewWriter(*conf)

	return &LogbrokerSender{
		writer: p,
	}, nil
}

func (s *LogbrokerSender) init(ctx context.Context) error {
	_, err := s.writer.Init(ctx)
	if err != nil {
		return xerrors.Errorf("can't init persqueue writer: %v", err)
	}

	go s.startReceivingFeedback()
	return nil
}

func (s *LogbrokerSender) startReceivingFeedback() {
	for resp := range s.writer.C() {
		switch m := resp.(type) {
		case *persqueue.Issue:
			logger.Log.Error("search: logbroker error feedback received", log.Error(m.Err))
		}
	}
}

func (s *LogbrokerSender) Send(ctx context.Context, res *SearchResource) error {
	data, err := json.Marshal(res)
	if err != nil {
		return xerrors.Errorf("marshal error: %v", err)
	}

	if err := s.writer.Write(&persqueue.WriteMessage{Data: data}); err != nil {
		return xerrors.Errorf("error writing to persqueue writer: %v", err)
	}

	return nil
}

func (s *LogbrokerSender) Close() error {
	return s.writer.Close()
}
