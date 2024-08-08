package reader

import (
	"context"

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/format"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type readerF = func(ctx context.Context, filePath string) (*S3Reader, error)

func estimateTotalSize(ctx context.Context, lgr log.Logger, files []*aws_s3.Object, readerF readerF) (uint64, *S3Reader, error) {
	var sampleReader *S3Reader
	multiplier := float64(1)
	sniffFiles := files
	if len(files) > EstimateFilesLimit {
		multiplier = float64(len(files)) / float64(EstimateFilesLimit)
		sniffFiles = files[:EstimateFilesLimit]
		lgr.Infof("there is to many files: %v, will sniff: %v files and multiply result on %v", len(files), EstimateFilesLimit, multiplier)
	}
	lgr.Infof("start to read: %v files in parralel", len(sniffFiles))
	sizes := make([]int64, len(sniffFiles))

	if err := util.ParallelDo(ctx, len(sniffFiles), 5, func(i int) error {
		file := sniffFiles[i]
		reader, err := readerF(ctx, *file.Key)
		if err != nil {
			return xerrors.Errorf("unable to open reader for file: %s: %w", *file.Key, err)
		}
		size := reader.Size()
		if size > 0 {
			sampleReader = reader
		}
		sizes[i] = size
		return nil
	}); err != nil {
		return 0, sampleReader, xerrors.Errorf("unable to estimate size: %w", err)
	}
	var totalSize uint64
	for _, s := range sizes {
		totalSize += uint64(s)
	}
	totalSize = uint64(float64(totalSize) * multiplier)

	if multiplier > 1 {
		lgr.Infof("size estimated: %v", format.SizeUInt64(totalSize))
	} else {
		lgr.Infof("size resolved: %v", format.SizeUInt64(totalSize))
	}
	return totalSize, sampleReader, nil
}
