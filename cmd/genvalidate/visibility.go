package main

import (
	"sort"

	"github.com/doublecloud/transfer/cloud/dataplatform/api/options"
)

type VisibilitySet map[options.Visibility]struct{}

var (
	specialValues = map[options.Visibility]struct{}{
		options.Visibility_VISIBILITY_VISIBLE: {},
		options.Visibility_VISIBILITY_HIDDEN:  {},
	}

	compositeVisibilities = map[options.Visibility][]options.Visibility{
		options.Visibility_VISIBILITY_CLOUD: {options.Visibility_VISIBILITY_RU, options.Visibility_VISIBILITY_KZ},
	}
)

func isSpecialVisibility(visibility options.Visibility) bool {
	_, isSpecial := specialValues[visibility]
	return isSpecial
}

func (v VisibilitySet) UnsupportedInstallations() []options.Visibility {
	unsupportedInstallations := make([]options.Visibility, 0, len(options.Visibility_value))
	for _, visibility := range options.Visibility_value {
		if isSpecialVisibility(options.Visibility(visibility)) {
			continue
		}
		if _, isComplex := compositeVisibilities[options.Visibility(visibility)]; isComplex {
			//will check children separately
			continue
		}
		if v.IsInvisibleAt(options.Visibility(visibility)) {
			unsupportedInstallations = append(unsupportedInstallations, options.Visibility(visibility))
		}
	}
	sort.Slice(unsupportedInstallations, func(i, j int) bool { return unsupportedInstallations[i] < unsupportedInstallations[j] })
	return unsupportedInstallations
}

func (v VisibilitySet) IsInvisibleAt(visibilityType options.Visibility) bool {
	if v.IsVisibleEverywhere() {
		return false
	}
	_, isVisible := v[visibilityType]
	return !isVisible
}

func (v VisibilitySet) IsVisibleEverywhere() bool {
	if len(v) == 0 {
		return true
	}
	return len(v) == len(options.Visibility_value)-len(specialValues)
}

func NewVisibilitySet(visibility []options.Visibility) VisibilitySet {
	visibilitySet := make(VisibilitySet)
	for _, item := range visibility {
		if isSpecialVisibility(item) {
			continue
		}
		visibilitySet[item] = struct{}{}
		if vals, isComplex := compositeVisibilities[item]; isComplex {
			//ad inner visibilities to map too
			for _, v := range vals {
				visibilitySet[v] = struct{}{}
			}
		}
	}
	return visibilitySet
}
