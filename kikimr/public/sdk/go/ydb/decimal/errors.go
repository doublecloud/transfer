package decimal

import "fmt"

var ErrSyntax = fmt.Errorf("invalid syntax")

type ParseError struct {
	Err   error
	Input string
}

func (p *ParseError) Error() string {
	return fmt.Sprintf(
		"decimal: parse %q: %v", p.Input, p.Err,
	)
}

func (p *ParseError) Unwrap() error {
	return p.Err
}

func syntaxError(s string) *ParseError {
	return &ParseError{
		Err:   ErrSyntax,
		Input: s,
	}
}

func precisionError(s string, precision, scale uint32) *ParseError {
	return &ParseError{
		Err:   fmt.Errorf("invalid precision/scale: %d/%d", precision, scale),
		Input: s,
	}
}
