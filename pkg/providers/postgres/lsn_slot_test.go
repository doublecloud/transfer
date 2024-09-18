package postgres

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueriesSchemaEscaping(t *testing.T) {
	schema := "testschema"
	create := fmt.Sprintf(`create table( if not exists)? %s\.__.+`, schema)
	insert := fmt.Sprintf(`insert into %s\.__.+.* values .+`, schema)
	update := fmt.Sprintf(`update %s\.__.+`, schema)
	selectQuery := fmt.Sprintf(`select .+ from %s\.__.+`, schema)
	delete := fmt.Sprintf(`delete from %s.__.+`, schema)

	require.Regexp(t, create, unifyString(GetInitLSNSlotDDL(schema)))
	require.Regexp(t, update, unifyString(GetMoveIteratorQuery(schema)))
	require.Regexp(t, update, unifyString(GetUpdateReplicatedQuery(schema)))
	require.Regexp(t, selectQuery, unifyString(GetSelectCommittedLSN(schema)))
	require.Regexp(t, insert, unifyString(GetInsertIntoMoleFinder(schema)))
	require.Regexp(t, delete, unifyString(GetDeleteFromMoleFinder(schema)))
	require.Regexp(t, insert, unifyString(GetInsertIntoMoleFinder2(schema)))
	require.Regexp(t, selectQuery, unifyString(GetSelectCommittedLSN2(schema)))
}

func unifyString(str string) string {
	return strings.ReplaceAll(strings.ToLower(str), "\n", " ")
}
