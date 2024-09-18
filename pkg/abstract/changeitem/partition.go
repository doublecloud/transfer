package changeitem

import (
	"fmt"
	"strings"
)

type Partition struct {
	Cluster   string `json:"cluster"`
	Partition uint32 `json:"partition"`
	Topic     string `json:"topic"`
}

func (p Partition) String() string {
	return fmt.Sprintf("{\"cluster\":\"%s\",\"partition\":%d,\"topic\":\"%s\"}", p.Cluster, p.Partition, p.Topic)
}

func (p Partition) LegacyShittyString() string {
	slashes := strings.Count(p.Topic, "/")
	oldFashionTopic := strings.ReplaceAll(strings.Replace(p.Topic, "/", "@", slashes-1), "/", "--")
	return fmt.Sprintf("rt3.%s--%s:%v", p.Cluster, oldFashionTopic, p.Partition)
}

func NewPartition(topicWithCluster string, partition uint32) Partition {
	if len(topicWithCluster) < 4 {
		return Partition{
			Partition: partition,
			Cluster:   "",
			Topic:     "",
		}
	}
	topicWithCluster = topicWithCluster[4:]

	splittedParts := strings.Split(topicWithCluster, "--")
	if len(splittedParts) == 1 {
		return Partition{
			Partition: partition,
			Cluster:   "",
			Topic:     "",
		}
	}

	topic := strings.Replace(topicWithCluster[len(splittedParts[0])+2:], "--", "/", -1)
	topic = strings.Replace(topic, "@", "/", -1)
	return Partition{
		Partition: partition,
		Cluster:   splittedParts[0],
		Topic:     topic,
	}
}
