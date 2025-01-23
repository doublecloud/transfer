package stringutil

import (
	"strings"
)

// HasPrefixCI checks if string has prefix in case-insensitive mode.
func HasPrefixCI(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[0:len(prefix)], prefix)
}

// TrimPrefixCI trims first found prefix in case-insensitive mode.
func TrimPrefixCI(s string, prefixes ...string) (string, bool) {
	for _, prefix := range prefixes {
		if HasPrefixCI(s, prefix) {
			return s[len(prefix):], true
		}
	}
	return "", false
}

type UTF8String interface {
	string | []byte
}

// TruncateUTF8 truncates utf-8 string.
func TruncateUTF8[T UTF8String](s T, limit int) T {
	if len(s) <= limit {
		return s
	}
	for limit > 0 && s[limit]&0b11000000 == 0b10000000 {
		limit--
	}
	return s[:limit]
}

func JoinStrings[T interface{}](separator string, handler func(item *T) string, items ...T) string {
	if len(items) == 0 {
		return ""
	}
	builder := strings.Builder{}
	builder.WriteString(handler(&items[0]))
	for i := 1; i < len(items); i++ {
		builder.WriteString(separator)
		builder.WriteString(handler(&items[i]))
	}
	return builder.String()
}
