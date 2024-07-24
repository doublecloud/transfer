package reader

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/parsers"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/s3"
	chunk_pusher "github.com/doublecloud/tross/transfer_manager/go/pkg/providers/s3/pusher"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/stats"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
)

var (
	_ Reader     = (*GenericParserReader)(nil)
	_ RowCounter = (*GenericParserReader)(nil)
)

type GenericParserReader struct {
	table       abstract.TableID
	bucket      string
	client      s3iface.S3API
	downloader  *s3manager.Downloader
	logger      log.Logger
	tableSchema *abstract.TableSchema
	pathPrefix  string
	blockSize   int64
	pathPattern string
	metrics     *stats.SourceStats
	parser      parsers.Parser
}

func (r *GenericParserReader) openReader(ctx context.Context, filePath string) (*S3Reader, error) {
	sr, err := NewS3Reader(ctx, r.client, r.downloader, r.bucket, filePath, r.metrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to create reader at: %w", err)
	}
	return sr, nil
}

func (r *GenericParserReader) estimateRows(ctx context.Context, files []*aws_s3.Object) (uint64, error) {
	res := uint64(0)
	var totalSize int64
	var sampleReader *S3Reader
	for _, file := range files {
		reader, err := r.openReader(ctx, *file.Key)
		if err != nil {
			return 0, xerrors.Errorf("unable to open reader for file: %s: %w", *file.Key, err)
		}
		size := reader.Size()
		if size > 0 {
			sampleReader = reader
		}
		totalSize += reader.Size()
	}

	if totalSize > 0 && sampleReader != nil {
		buff, err := r.ParseFile(ctx, "", sampleReader)
		if err != nil {
			return 0, xerrors.Errorf("unable to parse: %w", err)
		}
		bytesPerLine := float64(sampleReader.Size()) / float64(len(buff))
		totalLines := math.Ceil(float64(totalSize) / bytesPerLine)
		res = uint64(totalLines)
	}
	return res, nil
}

func (r *GenericParserReader) RowCount(ctx context.Context, obj *aws_s3.Object) (uint64, error) {
	res, err := r.estimateRows(ctx, []*aws_s3.Object{obj})
	if err != nil {
		return 0, xerrors.Errorf("failed to estimate rows of file: %s : %w", *obj.Key, err)
	}
	return res, nil
}

func (r *GenericParserReader) TotalRowCount(ctx context.Context) (uint64, error) {
	files, err := ListFiles(r.bucket, r.pathPrefix, r.pathPattern, r.client, r.logger, nil, r.IsObj)
	if err != nil {
		return 0, xerrors.Errorf("unable to load file list: %w", err)
	}

	res, err := r.estimateRows(ctx, files)
	if err != nil {
		return 0, xerrors.Errorf("failed to estimate total rows: %w", err)
	}
	return res, nil
}

func (r *GenericParserReader) Read(ctx context.Context, filePath string, pusher chunk_pusher.Pusher) error {
	s3Reader, err := r.openReader(ctx, filePath)
	if err != nil {
		return xerrors.Errorf("unable to open reader: %w", err)
	}

	readSize := s3Reader.Size()
	buff, err := r.ParseFile(ctx, filePath, s3Reader)
	if err != nil {
		return xerrors.Errorf("unable to parse: %s: %w", filePath, err)
	}

	if len(buff) > 0 {
		if err := pusher.Push(ctx, chunk_pusher.Chunk{
			Items:     buff,
			FilePath:  filePath,
			Offset:    0,
			Completed: true,
			Size:      readSize,
		}); err != nil {
			return xerrors.Errorf("unable to push: %w", err)
		}
	}
	return nil
}

func (r *GenericParserReader) ParsePassthrough(chunk chunk_pusher.Chunk) []abstract.ChangeItem {
	// the most complex and useful method in the world
	return chunk.Items
}

func (r *GenericParserReader) ParseFile(ctx context.Context, filePath string, s3Reader *S3Reader) ([]abstract.ChangeItem, error) {
	offset := 0
	var readBytes int

	var fullFile []byte
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("Read canceled")
			return nil, nil
		default:
		}
		data := make([]byte, r.blockSize)
		lastRound := false
		n, err := s3Reader.ReadAt(data, int64(offset))
		if err != nil {
			if xerrors.Is(err, io.EOF) && n > 0 {
				data = data[0:n]
				lastRound = true
			} else {
				return nil, xerrors.Errorf("failed to read from file: %w", err)
			}
		}
		offset += readBytes

		fullFile = append(fullFile, data...)
		if lastRound {
			break
		}
	}
	return r.parser.Do(persqueue.ReadMessage{
		Offset:      0,
		SeqNo:       0,
		SourceID:    nil,
		CreateTime:  s3Reader.LastModified(),
		WriteTime:   s3Reader.LastModified(),
		IP:          "",
		Data:        fullFile,
		Codec:       0,
		ExtraFields: nil,
	}, abstract.NewPartition(filePath, 0)), nil
}

func (r *GenericParserReader) ResolveSchema(ctx context.Context) (*abstract.TableSchema, error) {
	if r.tableSchema != nil && len(r.tableSchema.Columns()) != 0 {
		return r.tableSchema, nil
	}

	files, err := ListFiles(r.bucket, r.pathPrefix, r.pathPattern, r.client, r.logger, aws.Int(1), r.IsObj)
	if err != nil {
		return nil, xerrors.Errorf("unable to load file list: %w", err)
	}

	if len(files) < 1 {
		return nil, xerrors.Errorf("unable to resolve schema, no files found: %s", r.pathPrefix)
	}

	return r.resolveSchema(ctx, *files[0].Key)
}

func (r *GenericParserReader) IsObj(file *aws_s3.Object) bool {
	if file.Size == nil || *file.Size == 0 { // dir
		return false
	}
	return true
}

func (r *GenericParserReader) resolveSchema(ctx context.Context, key string) (*abstract.TableSchema, error) {
	s3Reader, err := r.openReader(ctx, key)
	if err != nil {
		return nil, xerrors.Errorf("unable to open reader for file: %s: %w", key, err)
	}

	buff := make([]byte, r.blockSize)
	read, err := s3Reader.ReadAt(buff, 0)
	if err != nil && !xerrors.Is(err, io.EOF) {
		return nil, xerrors.Errorf("failed to read sample from file: %s: %w", key, err)
	}
	if read == 0 {
		// read nothing, file was empty
		return nil, xerrors.New(fmt.Sprintf("could not read sample data from file: %s", key))
	}

	reader := bufio.NewReader(bytes.NewReader(buff))
	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, xerrors.Errorf("failed to read sample content for schema deduction: %w", err)
	}
	changes := r.parser.Do(persqueue.ReadMessage{Data: content}, abstract.NewPartition(key, 0))

	if len(changes) == 0 {
		return nil, xerrors.Errorf("unable to parse sample data: %v", util.Sample(string(content), 1024))
	}

	r.tableSchema = changes[0].TableSchema
	return r.tableSchema, nil
}

func NewGenericParserReader(src *s3.S3Source, lgr log.Logger, sess *session.Session, metrics *stats.SourceStats, parser parsers.Parser) (*GenericParserReader, error) {
	reader := &GenericParserReader{
		table: abstract.TableID{
			Namespace: src.TableNamespace,
			Name:      src.TableName,
		},
		bucket:      src.Bucket,
		client:      aws_s3.New(sess),
		downloader:  s3manager.NewDownloader(sess),
		logger:      lgr,
		tableSchema: abstract.NewTableSchema(src.OutputSchema),
		pathPrefix:  src.PathPrefix,
		blockSize:   1 * 1024 * 1024, // 1mb
		pathPattern: src.PathPattern,
		metrics:     metrics,
		parser:      parser,
	}

	if len(reader.tableSchema.Columns()) == 0 {
		var err error
		reader.tableSchema, err = reader.ResolveSchema(context.Background())
		if err != nil {
			return nil, xerrors.Errorf("unable to resolve schema: %w", err)
		}
	}

	return reader, nil
}
