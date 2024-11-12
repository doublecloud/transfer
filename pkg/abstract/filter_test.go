package abstract

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilterSimple(t *testing.T) {
	rawFilter := "Foo(created_at > '2017-01-01')"
	items, err := ParseFilterItems([]string{rawFilter})
	assert.NoError(t, err)
	assert.NotNil(t, items["Foo"])
	assert.Equal(t, items["Foo"], WhereStatement("created_at > '2017-01-01'"))
}

func TestFilterComplex(t *testing.T) {
	rawFilter := "Foo(created_at > '2017-01-01' and created_at <= '2018-01-01')"
	items, err := ParseFilterItems([]string{rawFilter})
	assert.NoError(t, err)
	assert.NotNil(t, items["Foo"])
	assert.Equal(t, items["Foo"], WhereStatement("created_at > '2017-01-01' and created_at <= '2018-01-01'"))
}

func TestEmpty(t *testing.T) {
	rawFilter := ""
	_, err := ParseFilterItems([]string{rawFilter})
	assert.NoError(t, err)
}

func TestMultiple(t *testing.T) {
	rawFilters := []string{
		"Foo(a = 123)",
		"Bar(b = 321)",
	}
	items, err := ParseFilterItems(rawFilters)
	assert.NoError(t, err)
	assert.Equal(t, len(items), 2)
	assert.Equal(t, items["Foo"], WhereStatement("a = 123"))
	assert.Equal(t, items["Bar"], WhereStatement("b = 321"))
}

func TestBadQueryNoBrackets(t *testing.T) {
	rawFilter := "BadQueryWithNowBrekets"
	_, err := ParseFilterItems([]string{rawFilter})
	assert.Error(t, err)
}

func TestBadQueryToManyBrackets(t *testing.T) {
	rawFilter := "BadQueryWithNowBrekets)()(()*"
	_, err := ParseFilterItems([]string{rawFilter})
	assert.Error(t, err)
}

func TestBadQueryInvalidOrderBrackets(t *testing.T) {
	rawFilter := "Bad)QueryWithNowBrekets("
	_, err := ParseFilterItems([]string{rawFilter})
	assert.Error(t, err)
}

func TestBadQueryClosedBracketNotLastSymbol(t *testing.T) {
	rawFilter := "Bad)QueryWithNowBrek(ets"
	_, err := ParseFilterItems([]string{rawFilter})
	assert.Error(t, err)
}

func TestQueryWithSubFunction(t *testing.T) {
	rawFilter := "client_task(created < now()::date AND created > now()::date - interval '1 day')"
	_, err := ParseFilterItems([]string{rawFilter})
	assert.NoError(t, err)
}

func TestQueryWithInvalidSubFunction(t *testing.T) {
	rawFilter := "client_task(created < now())::date AND created > now()::date - interval '1 day')"
	_, err := ParseFilterItems([]string{rawFilter})
	assert.Error(t, err)
}

func TestSum(t *testing.T) {
	tests := []struct {
		a        string
		b        string
		expected string
	}{
		// Test cases for positive numbers
		{"3", "5", "8"},
		{"100", "200", "300"},

		// Test cases for negative numbers
		{"-3", "-5", "-8"},
		{"-100", "-200", "-300"},

		// Test cases for mixed positive and negative numbers
		{"5", "-3", "2"},
		{"-5", "3", "-2"},
		{"-100", "50", "-50"},
		{"100", "-50", "50"},

		// Test cases involving zero
		{"0", "0", "0"},
		{"0", "5", "5"},
		{"5", "0", "5"},
		{"-5", "0", "-5"},
		{"0", "-5", "-5"},

		// Test cases with large numbers
		{"1000000000000000000", "1000000000000000000", "2000000000000000000"},
		{"-1000000000000000000", "1000000000000000000", "0"},
		{"1000000000000000000", "-1000000000000000000", "0"},

		// Test cases with edge numbers
		{"999999999999999999", "1", "1000000000000000000"},
		{"-999999999999999999", "-1", "-1000000000000000000"},
	}

	for _, tt := range tests {
		t.Run(tt.a+"_"+tt.b, func(t *testing.T) {
			result := sum(tt.a, tt.b)
			if result != tt.expected {
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func sum(a, b string) string {
	var res string
	aN := a[0] == '-'
	bN := b[0] == '-'
	if aN != bN {
		if aN {
			return minus(b, a[1:])
		} else {
			return minus(a, b[1:])
		}
	}
	if aN {
		a = a[1:]
		b = b[1:]
	}
	maxStr := a
	minStr := b
	minSize := len(b)
	if len(a) < len(b) {
		maxStr = b
		minStr = a
		minSize = len(a)
	}
	resSign := ""
	if aN {
		resSign = "-"
	}
	nextIncrease := 0
	for i := len(minStr) - 1; i >= 0; i-- {
		an, bn := minStr[i], maxStr[i+len(maxStr)-len(minStr)]
		ann, _ := strconv.Atoi(string(an))
		bnn, _ := strconv.Atoi(string(bn))
		sum := ann + bnn + nextIncrease
		nextIncrease = 0
		if sum >= 10 {
			nextIncrease = 1
		}
		cropSum := sum % 10
		res = fmt.Sprintf("%d", cropSum) + res
	}
	maxHead := maxStr[:len(maxStr)-minSize]
	for i := len(maxHead) - 1; i >= 0; i-- {
		lastChar := maxHead[i]
		ln, _ := strconv.Atoi(string(lastChar))

		if nextIncrease > 0 {
			ln++
			if ln < 10 {
				nextIncrease = 0
			}
		}
		cropSum := ln % 10
		res = fmt.Sprintf("%d", cropSum) + res
	}
	if nextIncrease > 0 {
		res = "1" + res
	}
	return resSign + res
}

func minus(a, b string) string {
	var res string
	remember := 0
	maxStr := a
	bTail := len(a) - len(b)
	aTail := len(b) - len(a)
	if len(a) > len(b) {
		aTail = 0
		maxStr = a
	} else {
		bTail = 0
		maxStr = b
	}
	for i := len(maxStr) - 1; i >= 0; i-- {
		ann := 0
		if i-aTail >= 0 {
			ann, _ = strconv.Atoi(string(a[i-aTail]))

		}
		bnn := 0
		if i-bTail >= 0 {
			bnn, _ = strconv.Atoi(string(b[i-bTail]))

		}

		r := ann - bnn - remember
		if r < 0 {
			remember = 1
		}
		if i > 0 || r != 0 {
			res = fmt.Sprintf("%v", math.Abs(float64(r%10))) + res
		}
	}
	if len(a) > len(b) {
		return res
	}
	if len(a) == len(b) && remember == 0 {
		return res
	}
	return "-" + res
}
