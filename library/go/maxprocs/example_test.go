package maxprocs_test

import "github.com/doublecloud/tross/library/go/maxprocs"

func Example() {
	// Если ты в Qloud
	maxprocs.AdjustQloud()

	// Если ты в Y.Deploy (aka YP/YP.Hard)
	maxprocs.AdjustYP()

	// Если ты в RTC c YP.Lite
	maxprocs.AdjustYPLite()

	// Если ты в RTC c InstanceCTL версии 1.177+
	maxprocs.AdjustInstancectl()

	// Во всех остальных случаях
	maxprocs.AdjustAuto()
}
