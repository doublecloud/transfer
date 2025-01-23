package glob

import (
	"bytes"
	"strings"
	"unsafe"
)

func SplitMatch(pattern, subj, splitter string) bool {
	// Empty pattern can only match empty subject
	if pattern == "" {
		return true
	}

	// If the pattern _is_ a glob, it matches everything
	if pattern == "*" {
		return true
	}
	if pattern == "**" {
		return true
	}

	parts := strings.Split(pattern, splitter)
	matched := false
	for _, part := range parts {
		if Match(part, subj) {
			matched = true
			break
		}
	}

	return matched
}

func Match(pattern, subj string) bool {
	// Empty pattern can only match empty subject
	if pattern == "" {
		return subj == pattern
	}

	// If the pattern _is_ a glob, it matches everything
	if pattern == "*" {
		return true
	}

	// get underlying bytes of pattern
	patternBytes := unsafe.Slice(unsafe.StringData(pattern), len(pattern))

	// get underlying bytes of subject
	subjBytes := unsafe.Slice(unsafe.StringData(subj), len(subj))

	for {
		// find first glob index
		idx := bytes.IndexByte(patternBytes, '*')
		if idx == -1 {
			// check remaining tail for match
			return bytes.HasSuffix(subjBytes, patternBytes)
		}

		// find index of part between glob(s)
		part := patternBytes[:idx]
		if len(part) == 0 {
			// skip consecutive globs
			patternBytes = patternBytes[idx+1:]
			continue
		}

		// find matching part in subject
		match := bytes.Index(subjBytes, part)
		if len(pattern) == len(patternBytes) {
			// check non-leading glob pattern of first iteration
			if patternBytes[0] != '*' && match != 0 {
				return false
			}
		} else if match == -1 {
			// check simple match
			return false
		}

		// trim checked parts of pattern and subject before next iteration
		patternBytes = patternBytes[idx+1:]
		subjBytes = subjBytes[match+len(part):]
	}
}
