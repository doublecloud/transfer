package logbroker

import (
	"bytes"
	"context"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

type CancelableWriter interface {
	Write(context.Context, []byte) error
	Close(context.Context) error
	Cancel()
	GroupID() string
	SetGroupID(groupID string)
}

type persqueueWriter struct {
	groupID    string
	writer     persqueue.Writer
	cancelFunc context.CancelFunc
}

var _ CancelableWriter = (*persqueueWriter)(nil)

func (p *persqueueWriter) Write(ctx context.Context, data []byte) error {
	return p.writer.Write(&persqueue.WriteMessage{Data: data})
}

func (p *persqueueWriter) Close(ctx context.Context) error {
	return p.writer.Close()
}

func (p *persqueueWriter) Cancel() {
	p.cancelFunc()
}

func (p *persqueueWriter) GroupID() string {
	return p.groupID
}

func (p *persqueueWriter) SetGroupID(groupID string) {
	p.groupID = groupID
}

type topicWriter struct {
	topicWriter *topicwriter.Writer
	cancelFunc  context.CancelFunc
	groupID     string
}

var _ CancelableWriter = (*topicWriter)(nil)

func (t *topicWriter) Write(ctx context.Context, data []byte) error {
	return t.topicWriter.Write(ctx, topicwriter.Message{Data: bytes.NewReader(data)})
}

func (t *topicWriter) Close(ctx context.Context) error {
	return t.topicWriter.Close(ctx)
}

func (t *topicWriter) Cancel() {
	t.cancelFunc()
}

func (t *topicWriter) GroupID() string {
	return t.groupID
}

func (t *topicWriter) SetGroupID(groupID string) {
	t.groupID = groupID
}
