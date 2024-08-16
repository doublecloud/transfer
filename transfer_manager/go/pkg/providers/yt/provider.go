package yt

import (
	"encoding/gob"

	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
)

func init() {
	destinationFactory := func() server.Destination {
		return &YtDestinationWrapper{
			Model:    new(YtDestination),
			_pushWal: false,
			_noBan:   false,
		}
	}
	destinationCopyFactory := func() server.Destination {
		return new(YtCopyDestination)
	}
	stagingFactory := func() server.Destination {
		return new(LfStagingDestination)
	}

	gob.RegisterName("*server.YtDestination", new(YtDestination))
	gob.RegisterName("*server.YtDestinationWrapper", new(YtDestinationWrapper))
	gob.RegisterName("*server.YtSource", new(YtSource))
	gob.RegisterName("*server.YtCopyDestination", new(YtCopyDestination))
	gob.RegisterName("*server.LfStagingDestination", new(LfStagingDestination))

	server.RegisterDestination(ProviderType, destinationFactory)
	server.RegisterDestination(StagingType, stagingFactory)
	server.RegisterDestination(CopyType, destinationCopyFactory)
	server.RegisterSource(ProviderType, func() server.Source {
		return new(YtSource)
	})

	abstract.RegisterProviderName(ProviderType, "YT")
	abstract.RegisterProviderName(StagingType, "Logfeller staging area")
	abstract.RegisterProviderName(CopyType, "YT Copy")

	abstract.RegisterSystemTables(TableProgressRelativePath, TableWAL)
}

const (
	TableProgressRelativePath = abstract.TableLSN // "__data_transfer_lsn"
	TableWAL                  = "__wal"

	ProviderType = abstract.ProviderType("yt")
	StagingType  = abstract.ProviderType("lfstaging")
	CopyType     = abstract.ProviderType("ytcopy")
)
