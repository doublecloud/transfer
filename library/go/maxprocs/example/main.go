package main

import (
	"fmt"
	"runtime"

	"github.com/doublecloud/tross/library/go/maxprocs"
)

func main() {
	maxprocs.AdjustAuto()
	fmt.Printf("GOMAXPROCS is: %d\n", runtime.GOMAXPROCS(0))
}
