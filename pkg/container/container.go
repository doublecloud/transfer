package container

import (
	"context"
	"io"
	"os"

	"github.com/docker/docker/api/types"
	"go.ytsaurus.tech/library/go/core/log"
)

type ContainerImpl interface {
	Run(context.Context, ContainerOpts) (io.Reader, io.Reader, error)
	Pull(context.Context, string, types.ImagePullOptions) error
}

func NewContainerImpl(l log.Logger) (ContainerImpl, error) {
	if isRunningInKubernetes() {
		return NewK8sWrapper()
	}

	return NewDockerWrapper(l)
}

func isRunningInKubernetes() bool {
	_, exists := os.LookupEnv("KUBERNETES_SERVICE_HOST")
	return exists
}
