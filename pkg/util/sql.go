package util

func SplitStatements(s string) []string {
	var statements []string
	isQuote := func(x rune) bool {
		return x == '"' || x == '`' || x == '\''
	}
	i := 0
	var quote rune = 0
	escaped := false
	for j, char := range s {
		if !escaped {
			if isQuote(char) {
				if quote == 0 {
					quote = char
				} else if quote == char {
					quote = 0
				}
			} else if char == ';' && quote == 0 && i < j {
				statements = append(statements, s[i:j])
				i = j + 1
			}
		}

		if quote != 0 && char == '\\' && !escaped {
			escaped = true
		} else {
			escaped = false
		}
	}
	if i < len(s) {
		statements = append(statements, s[i:])
	}
	return statements
}
