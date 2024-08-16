package persqueue

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/credentials"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/log"
)

// WriteMessage is struct for incoming message
type WriteMessage struct {
	CreateTimestamp time.Time
	Data            []byte

	assignedSeqNo uint64
}

// WriterInit is struct that show reader start position.
type WriterInit struct {
	// MaxSeqNo is stored SeqNo.
	MaxSeqNo uint64
	// SessionID is session mark. Use full for debug purpose.
	SessionID string
	// Topic is a name.
	Topic     string
	Partition uint64
	Cluster   string
}

// WriteResponse is dummy interface, holds either Ack or Issue.
type WriteResponse interface {
	isWriteResponse()
}

type writeResponse struct{}

func (w *writeResponse) isWriteResponse() {}

// WithSeqNo assign sequence number to the message.
// Must be growing for each new message.
// Used by Logbroker to deduplicate inputstream.
func (w *WriteMessage) WithSeqNo(seqNo uint64) *WriteMessage {
	w.assignedSeqNo = seqNo
	return w
}

// GetAssignedSeqNo returns assigned sequence number to the message.
// If seq no is not assignedd then returns zero
func (w *WriteMessage) GetAssignedSeqNo() uint64 {
	return w.assignedSeqNo
}

// Ack is struct that contains acknowledge about written message
type Ack struct {
	// SeqNo is mark that may be used for client side deduplication.
	SeqNo uint64

	// Offset might be missing, for messages written just before reconnect.
	Offset         uint64
	AlreadyWritten bool

	writeResponse
}

var _ WriteResponse = (*Ack)(nil)

type Issue struct {
	Err error

	Data *WriteMessage

	writeResponse
}

var _ WriteResponse = (*Issue)(nil)

// Writer is sending messages to log broker.
type Writer interface {
	// Init initializes writer. Writer is terminated abnormally, when provided ctx is canceled.
	Init(ctx context.Context) (init *WriterInit, err error)

	// Write sends single message.
	//
	// Write is concurrency-safe.
	//
	// Write blocks, when writer internal message buffer reaches memory limit.
	Write(d *WriteMessage) error

	// Close flushes current messages to the server and stops the writer, waiting for correct termination.
	//
	// It is not safe to call Close() concurrently with Write().
	//
	// Do not use this method, if you want to abort writer abnormally, cancel ctx passed to Init.
	Close() error

	// C returns a channel emitting writer feedback messages.
	//
	// Client must read messages from this channel in a timely manner, otherwise writer would block.
	C() <-chan WriteResponse

	// Closed return channel, that is closed when writer is stopped.
	Closed() <-chan struct{}

	// Stat returns writer statistics.
	//
	// This method is safe to call from any goroutine.
	Stat() WriterStat
}

type WriterOptions struct {
	// Credentials. Must provide token.
	Credentials credentials.Credentials

	// Database.
	Database string

	// TLSConfig specifies the TLS configuration to use for tls client.
	// If TLSConfig is zero then connections are insecure.
	TLSConfig *tls.Config

	// Endpoint of the logbroker.
	//
	// See https://lb.yandex-team.ru/docs/concepts/clusters_and_installations/
	Endpoint string

	// Port corresponding to Endpoint. If 0, default port is used.
	Port int

	// Logger implementation. By default simple console logger
	Logger log.Logger

	// Will make retry for all network / server related issues until graceful shutdown
	RetryOnFailure bool

	// Topic where data will be written
	Topic string

	// Partition groups, by default 0 - any partition
	PartitionGroup uint32

	// Source ID will be used as key for partition assignment.
	SourceID []byte

	// Compressor codec, by default RAW - no compression.
	Codec Codec

	// Number of goroutines allocated for compression.
	// Default (0) is runtime.NumCPU() - 1
	CompressionConcurrency int

	// Extra settings, will be attached to every written message
	ExtraAttrs map[string]string

	// Maximum memory (bytes) that can be used by writer.
	// If limit exceeded write will be throttled.
	// Default 0 - no limits.
	MaxMemory int

	// ClientTimeout is a timeout for client grpc call.
	// Default 5 seconds.
	ClientTimeout time.Duration

	// Compression level should be compatible with "compress/flate" except
	// for one value. Zero value is semantically equal to flake.BestCompression.
	// Default 0 - flake.BestCompression.
	CompressionLevel int

	// DiscoverCluster enables cluster discovery via logbroker Cluster Discovery Service
	DiscoverCluster bool

	// PreferredClusterName for cluster discovery
	PreferredClusterName string

	proxy string

	// CompressorPoolProvider allows to control/share compressors accross several writers.
	// Use NewCompressorPool to create a new pool.
	// Default null - a new separate pool will be used.
	CompressorPoolProvider CompressorPoolProvider
}

func (o WriterOptions) WithProxy(proxy string) WriterOptions {
	o.proxy = proxy
	return o
}

func (o WriterOptions) Proxy() string {
	return o.proxy
}

// ReceiveIssues is helper function which consume all available issues.
func ReceiveIssues(p Writer) (issues []*Issue) {
	for rsp := range p.C() {
		switch m := rsp.(type) {
		case *Ack:
		case *Issue:
			issues = append(issues, m)
		}
	}

	return
}

// WriterStat is struct that shows simple writer statistic
type WriterStat struct {
	// MemUsage is amount of RAM in bytes currently in flight (send to server, with no ack).
	MemUsage int
	// Inflight is count of message currently in flight.
	Inflight int
}
