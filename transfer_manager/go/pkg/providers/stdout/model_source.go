package stdout

import (
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
)

type EmptySource struct{}

var _ server.Source = (*EmptySource)(nil)

func (EmptySource) WithDefaults() {
}

func (s *EmptySource) Include(tID abstract.TableID) bool {
	return len(s.FulfilledIncludes(tID)) > 0
}

func (*EmptySource) FulfilledIncludes(tID abstract.TableID) []string {
	return []string{""}
}

func (*EmptySource) AllIncludes() []string {
	return nil
}

func (EmptySource) IsSource() {
}

func (s *EmptySource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (s *EmptySource) Validate() error {
	return nil
}
