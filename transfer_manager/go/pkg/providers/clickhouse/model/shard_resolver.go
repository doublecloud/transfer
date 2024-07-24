package model

import (
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/dbaas"
)

func ShardFromCluster(clusterID string) (map[string][]string, error) {
	hosts, err := dbaas.ResolveClusterHosts(dbaas.ProviderTypeClickhouse, clusterID)
	if err != nil {
		return nil, xerrors.Errorf("unable to list hosts: %w", err)
	}
	shards := make(map[string][]string)
	for _, h := range hosts {
		if shards[h.ShardName] == nil {
			shards[h.ShardName] = make([]string, 0)
		}
		shards[h.ShardName] = append(shards[h.ShardName], h.Name)
	}
	return shards, nil
}
