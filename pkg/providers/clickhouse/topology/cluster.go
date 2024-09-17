package topology

type ShardHostMap map[int][]string

type Cluster struct {
	Topology Topology
	Shards   ShardHostMap
}

func (c *Cluster) Name() string {
	return c.Topology.ClusterName()
}

func (c *Cluster) SingleNode() bool {
	return c.Topology.SingleNode()
}
