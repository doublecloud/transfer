package main

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

func camelToSnake(s string) string {
	var b strings.Builder
	b.Grow(len(s))

	buf := make([]byte, utf8.UTFMax)
	write := func(c rune) {
		n := utf8.EncodeRune(buf, c)
		b.Write(buf[:n])
	}

	type char struct {
		value rune
		upper bool
	}
	var (
		prev   [2]rune
		lodash bool
	)
	advance := func(c rune) {
		if prev[0] == 0 {
			return
		}

		u0 := unicode.IsUpper(prev[0])
		u1 := unicode.IsUpper(prev[1])

		switch {
		case u0 && !u1:
			if b.Len() > 0 {
				b.WriteByte('_')
			}
			write(unicode.ToLower(prev[0]))
			prev[0] = prev[1]
			prev[1] = c

		case u0 && u1:
			if lodash {
				b.WriteByte('_')
				lodash = false
			}
			write(unicode.ToLower(prev[0]))
			if c > 0 && !unicode.IsUpper(c) {
				b.WriteByte('_')
			}
			write(unicode.ToLower(prev[1]))

			prev[0] = c
			prev[1] = 0

		default:
			lodash = !u0 && u1
			write(prev[0])
			prev[0] = prev[1]
			prev[1] = c
		}
	}
	for i := 0; i < len(s); {
		c, n := utf8.DecodeRuneInString(s[i:])
		i += n

		if prev[0] == 0 {
			prev[0] = c
			continue
		}
		if prev[1] == 0 {
			prev[1] = c
			continue
		}

		advance(c)
	}

	advance(0)
	advance(0)

	return b.String()
}
