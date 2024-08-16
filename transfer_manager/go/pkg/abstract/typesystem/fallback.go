package typesystem

import (
	"fmt"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
)

// LatestVersion is the current (most recent) version of the typesystem. Increment this when adding a new fallback.
//
// At any moment, fallbacks can only be "to" any version preceding the latest.
//
// Zero value is reserved and MUST NOT be used.
//
// When incrementing this value, DO ADD a link to the function(s) implementing this fallback to CHANGELOG.md in the current directory
const LatestVersion int = 9

// NewTransfersVersion is the version of the typesystem set for new transfers. It must be less or equal to the LatestVersion.
//
// To upgrade typesystem version, the following process should be applied:
// 1. LatestVersion is increased & fallbacks are introduced in the first PR. NewTransfersVersion stays the same!
// 2. Controlplane and dataplane are deployed and dataplane now contains the fallbacks for a new version.
// 3. The second PR increases NewTransfersVersion. When a controlplane with this change is deployed, dataplanes already have the required fallbacks.
const NewTransfersVersion int = 9

var FallbackDoesNotApplyErr = xerrors.NewSentinel("this fallback does not apply")

type FallbackFactory func() Fallback

// Fallback defines a transformation from a newer version of typesystem to an older one.
type Fallback struct {
	// To is the target typesystem version of this fallback
	To int
	// ProviderType limits the providers to which this fallback applies
	ProviderType abstract.ProviderType
	// Function defines the transformation. Input is an item of any kind.
	//
	// If a fallback does not apply, this method MUST return an error containing FallbackDoesNotApplyErr
	Function func(ci *abstract.ChangeItem) (*abstract.ChangeItem, error)
}

func (f Fallback) Applies(version int, providerType abstract.ProviderType) bool {
	return f.ProviderType == providerType && f.To >= version
}

func (f Fallback) String() string {
	return fmt.Sprintf("fallback [%q->%d, %p]", f.ProviderType.Name(), f.To, f.Function)
}

func (f Fallback) Equals(other Fallback) bool {
	return f.ProviderType == other.ProviderType && f.To == other.To
}

// AddFallbackSourceFactory registers a fallbacks for a source of some type
//
// This method is expected to be called in the `init()` function of a module which introduces a fallback when some additional behaviour is expected
func AddFallbackSourceFactory(factory FallbackFactory) {
	registerFallbackFactory(&SourceFallbackFactories, factory)
}

// AddFallbackTargetFactory registers a fallbacks for a target of some type
//
// This method is expected to be called in the `init()` function of a module which introduces a fallback when some additional behaviour is expected
func AddFallbackTargetFactory(factory FallbackFactory) {
	registerFallbackFactory(&TargetFallbackFactories, factory)
}
