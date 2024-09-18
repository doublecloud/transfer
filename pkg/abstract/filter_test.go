package abstract

import (
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
