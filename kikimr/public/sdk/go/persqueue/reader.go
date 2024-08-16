package persqueue

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/credentials"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/log"
)

// ReaderInit is data describing current read session
type ReaderInit struct {
	SessionID string
}

// Reader is reading messages from logbroker.
type Reader interface {
	// Start initializes and starts the reader.
	Start(ctx context.Context) (*ReaderInit, error)

	// Shutdown stops receiving new messages from logbroker.
	Shutdown()

	// Closed returns a channel that is closed when reader is stopped. Either because of error of due to proper shutdown.
	Closed() <-chan struct{}

	// Err returns cause of reader termination.
	Err() error

	// C returns stream of reader events. Channel is closed when reader is terminated.
	//
	// Client MUST receive all messages from the channel.
	C() <-chan Event

	// Stat returns reader statistics.
	Stat() Stat
}

type ReaderOptions struct {
	// Credentials. Must provide token.
	Credentials credentials.Credentials

	// Database.
	Database string

	// TLSConfig specifies the TLS configuration to use for tls client.
	// If TLSConfig is zero then connections are insecure.
	TLSConfig *tls.Config `json:"-"` // if TLSConfig is not nil - json can't marshal it.

	// Endpoint of the logbroker.
	//
	// See https://lb.yandex-team.ru/docs/concepts/clusters_and_installations/
	Endpoint string

	// Optional port.
	Port int

	// Logger implementation. By default simple console logger
	Logger log.Logger

	// Reader identifier. Must be registered in the logbroker ahead of time.
	Consumer string

	// Topics to read
	Topics []TopicInfo

	// ReadOnlyLocal disables read of mirrored partitions.
	ReadOnlyLocal bool

	// Will send lock and release commands.
	// You must lock every partition before you start reads.
	ManualPartitionAssignment bool
	// Persqueue server will not wait for inflight reads in case of re-balance.
	ForceRebalance bool

	// Maximum memory that can be used by reader.
	// If limit exceeded read will be throttled.
	// Default 0 - no limits.
	MaxMemory int

	// Maximum message count to be read in one message batch
	MaxReadMessagesCount uint32
	// Maximum data size to be read in one message batch
	MaxReadSize uint32
	// Number of partitions to read
	MaxReadPartitionsCount uint32
	// Maximum `age` of message. Every message older then provided will be ignored.
	// Default 0, all available message.
	MaxTimeLag time.Duration

	// Init reading from given timestamp. If this timestamp already committed - this parameter will be ignored.
	// So you can't return back via this parameter.
	// So you can use this option together with RetryOnFailure.
	ReadTimestamp time.Time

	// ClientTimeout is a timeout for client grpc call.
	// Default 15 seconds.
	ClientTimeout time.Duration

	// CommitsDisabled disables client commits.
	//
	// When this option is set, server considers message committed, when it is sent to the client.
	CommitsDisabled bool

	// DecompressionDisabled disables decompression of received messages, passing then as is.
	DecompressionDisabled bool

	// Will make retry for all network / server related issues until graceful shutdown
	RetryOnFailure bool

	proxy string
}

func (o ReaderOptions) GetTopics() []string {
	res := make([]string, len(o.Topics))

	for idx, topic := range o.Topics {
		res[idx] = topic.Topic
	}

	return res
}

func (o ReaderOptions) GetPartitionGroups() []uint32 {
	res := make([]uint32, 0)
	for _, topic := range o.Topics {
		res = append(res, topic.PartitionGroups...)
	}

	return res
}

func (o ReaderOptions) WithProxy(proxy string) ReaderOptions {
	o.proxy = proxy
	return o
}

func (o ReaderOptions) Proxy() string {
	return o.proxy
}

// Stat is struct shows current reader usage statistic
type Stat struct {
	// MemUsage is amount of RAM in bytes currently in flight (read with no ack).
	MemUsage int
	// InflightCount is count of message currently in flight.
	InflightCount int
	// WaitAckCount is count of messages that mark as read at client but no ack
	WaitAckCount int

	// BytesExtracted growing counter of extracted bytes by reader
	BytesExtracted uint64
	// BytesRead growing counter of raw read bytes by reader
	BytesRead uint64
	SessionID string
}

// Event is generic interface for all reader events
type Event interface {
	isEvent()
}

// DataMessage prefer it if you want to decouple your code from persqueue implementation details
type DataMessage interface {
	Event
	Commit()
	Batches() []MessageBatch
	GetCookie() uint64
	PartitionCookie() PartitionCookie
}

var _ DataMessage = (*Data)(nil)

type consumerEvent struct{}

func (*consumerEvent) isEvent() {}

// TODO(prime@): this message is useless in the current form.
type CommitAck struct {
	Cookies          []uint64
	PartitionCookies []PartitionCookie

	consumerEvent
}

// ReadMessage is struct describing incoming message
type ReadMessage struct {
	// Offset is server sequence of message in topic. Must be monotone growing.
	Offset uint64
	// SeqNo is client set mark of message. Must be growing.
	SeqNo    uint64
	SourceID []byte

	CreateTime time.Time
	WriteTime  time.Time
	IP         string

	Data  []byte
	Codec Codec

	ExtraFields map[string]string
}

// MessageBatch is group of messages.
type MessageBatch struct {
	Topic     string
	Partition uint32
	Messages  []ReadMessage
}

type PartitionCookie struct {
	AssignID        uint64
	PartitionCookie uint64
}

// Data is message batch group.
type Data struct {
	Cookie uint64

	partitionCookie PartitionCookie
	batches         []MessageBatch
	commitBuffer    *commitBuffer

	consumerEvent
}

func (m *Data) PartitionCookie() PartitionCookie {
	return m.partitionCookie
}

func (m *Data) size() (size int) {
	for _, b := range m.Batches() {
		for _, m := range b.Messages {
			size += len(m.Data)
		}
	}

	return
}

func (m *Data) GetCookie() uint64 {
	return m.Cookie
}

// Batches return array of batches that contains in Data.
func (m *Data) Batches() []MessageBatch {
	return m.batches
}

// Commit enqueue data to commit in on server.
func (m *Data) Commit() {
	m.commitBuffer.EnqueueCommit(m.partitionCookie, m.size())
	m.commitBuffer = nil
}

// Lock is struct describing information about server lock.
type Lock struct {
	Topic      string
	Partition  uint32
	ReadOffset uint64
	EndOffset  uint64
	Generation uint64

	lockC chan lockRequest

	consumerEvent
}

type lockRequest struct {
	lock             *Lock
	verifyReadOffset bool

	readOffset   uint64
	commitOffset uint64
}

// StartRead signals to the server, that client is ready to receive messages for the corresponding partition.
//
// Client must specify readOffset and commitOffset. In the simple case, both values should be equal to the value of the ReadOffset field.
func (l *Lock) StartRead(verifyReadOffset bool, readOffset, commitOffset uint64) {
	// TODO(prime@): possible deadlock here.
	l.lockC <- lockRequest{
		l,
		verifyReadOffset,
		readOffset,
		commitOffset,
	}

	l.lockC = nil
}

// Lock for v1 protocol, also contains AssignID and Cluster
type LockV1 struct {
	Topic      string
	Cluster    string
	AssignID   uint64
	Partition  uint64
	ReadOffset uint64
	EndOffset  uint64

	lockC chan lockV1Request

	consumerEvent
}

type lockV1Request struct {
	lock             *LockV1
	verifyReadOffset bool

	readOffset   uint64
	commitOffset uint64
}

// StartRead signals to the server, that client is ready to receive messages for the corresponding partition.
//
// Client must specify readOffset and commitOffset. In the simple case, both values should be equal to the value of the ReadOffset field.
func (l *LockV1) StartRead(verifyReadOffset bool, readOffset, commitOffset uint64) {
	// TODO(prime@): possible deadlock here.
	l.lockC <- lockV1Request{
		l,
		verifyReadOffset,
		readOffset,
		commitOffset,
	}

	l.lockC = nil
}

type releaseV1Request struct {
	Topic     string
	Cluster   string
	Partition uint64
	AssignID  uint64
}

type ReleaseV1 struct {
	Topic        string
	Partition    uint64
	CanCommit    bool
	Generation   uint64
	Cluster      string
	CommitOffset uint64

	releaseC chan releaseV1Request
	consumerEvent
}

func (r *ReleaseV1) Release() {
	r.releaseC <- releaseV1Request{
		Topic:     r.Topic,
		Cluster:   r.Cluster,
		Partition: r.Partition,
		AssignID:  r.Generation,
	}
	r.releaseC = nil
}

// PartitionStatus is a Response for status request.
type PartitionStatus struct {
	Cluster         string
	AssignID        uint64
	Partition       uint64
	Topic           string
	CommittedOffset uint64
	EndOffset       uint64

	consumerEvent
}

// Release is struct describing information about free up reader.
type Release struct {
	Topic      string
	Partition  uint32
	CanCommit  bool
	Generation uint64

	consumerEvent
}

// Disconnect is message that is emitted by reader when connection to the server is lost.
type Disconnect struct {
	Err error

	consumerEvent
}
