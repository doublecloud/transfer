package filter

import (
	"fmt"

	"github.com/alecthomas/participle/lexer"
)

type SyntaxError struct {
	Message string
}

func (e *SyntaxError) Error() string {
	return e.Message
}

func errorPrefix(pos lexer.Position) string {
	if pos.Line > 1 {
		return fmt.Sprintf("filter syntax error at or near line %d column %d: ", pos.Line, pos.Column)
	}
	return fmt.Sprintf("filter syntax error at or near column %d: ", pos.Column)
}

func newSyntaxError(pos lexer.Position, message string) error {
	return &SyntaxError{errorPrefix(pos) + message}
}

func newSyntaxErrorf(pos lexer.Position, format string, a ...interface{}) error {
	return &SyntaxError{fmt.Sprintf(errorPrefix(pos)+format, a...)}
}
