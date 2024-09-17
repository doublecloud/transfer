package util

import (
	"fmt"
	"runtime"
	"strings"
)

func GetCurrentGoroutineCallstack() string {
	buf := make([]byte, 1<<16)
	bufLen := runtime.Stack(buf, false)
	return string(buf[0:bufLen])
}

func GetMiniCallstack(depth int) []string {
	// get caller info
	var miniCallstack []string
	// 0 is current function, 1 is parent function calling getMiniCallstack, so we begin with 1
	for offset := 1; offset <= depth; offset++ {
		_, file, no, ok := runtime.Caller(offset)
		if ok {
			caller := ""
			callerPath := strings.Split(fmt.Sprintf("%s:%d", file, no), "/")
			if len(callerPath) > 1 {
				caller = strings.Join(callerPath[len(callerPath)-2:], "/")
			} else {
				caller = strings.Join(callerPath, "/")
			}
			miniCallstack = append(miniCallstack, caller)
		} else {
			break
		}
	}
	return miniCallstack
}
