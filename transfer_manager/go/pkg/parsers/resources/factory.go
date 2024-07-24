package resources

import "github.com/doublecloud/tross/library/go/core/log"

var NewResources = func(logger log.Logger, resourceNames []string) (AbstractResources, error) {
	logger.Info("creating NoResources", log.Any("resourceNames", resourceNames))
	return &NoResources{}, nil
}
