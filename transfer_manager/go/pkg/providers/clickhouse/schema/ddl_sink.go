package schema

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
)

var createDDLre = regexp.MustCompile(`(?mis)` + // multiline, ignore case, dot matches new line
	`(create\s+(table|materialized\s+view)(\s+if\s+not\s+exists)?\s+(.+?))` + // create table/mv and name
	"(\\s+uuid\\s'[^']+')?(\\s+on\\s+cluster(\\s+[^\\s]+|\\s+`[^`]+`|\\s+\"[^\"]+\"|))?" + // uuid, on cluster optional clauses
	`\s*\((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*\)\s*` + // table guts
	`engine\s*=\s*(([^\s]+\s*\([^)]+\))|([^\s]+))`, // engine
)

func extractNameClusterEngine(createDdlSQL string) (createClause, onClusterClause, engineStr string, found bool) {
	createDdlSQL = strings.TrimRight(createDdlSQL, "\n\r\t ;")
	res := createDDLre.FindAllStringSubmatch(createDdlSQL, -1)
	if res == nil || len(res) > 1 {
		return createClause, onClusterClause, engineStr, found
	}

	createClause = res[0][1]
	onClusterClause = res[0][6]
	engineStr = res[0][8]
	found = true

	return createClause, onClusterClause, engineStr, found
}

func IsDistributedDDL(sql string) bool {
	_, onCluster, _, found := extractNameClusterEngine(sql)
	if !found {
		return false
	}

	return strings.Trim(onCluster, " \t\n\r") != ""
}

func ReplaceCluster(sql, cluster string) string {
	_, onCluster, _, found := extractNameClusterEngine(sql)
	if !found {
		return sql
	}
	if strings.Count(sql, onCluster) > 1 {
		// something went wrong
		return sql
	}

	return strings.Replace(sql, onCluster, fmt.Sprintf(" ON CLUSTER `%s`", cluster), 1)
}

func SetReplicatedEngine(sql, baseEngine, db, table string) (string, error) {
	if IsReplicatedEngineType(baseEngine) {
		return sql, nil
	}

	engine, engineStr, err := ParseMergeTreeFamilyEngine(sql)
	if err != nil {
		return "", xerrors.Errorf("unable to parse engine from ddl: %w", err)
	}
	if EngineType(baseEngine) != engine.Type {
		return "", xerrors.Errorf("parsed engine(%v) is not equal with passed engine(%v)", engine.Type, baseEngine)
	}

	replicatedEngine, err := NewReplicatedEngine(engine, db, table)
	if err != nil {
		return "", xerrors.Errorf("unable to make replicated engine: %w", err)
	}
	return strings.Replace(sql, engineStr, replicatedEngine.String(), 1), nil
}

func SetIfNotExists(sql string) string {
	if !strings.Contains(sql, "IF NOT EXISTS") {
		switch {
		case strings.Contains(sql, "CREATE TABLE"):
			sql = strings.Replace(sql, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
		case strings.Contains(sql, "CREATE MATERIALIZED VIEW"):
			sql = strings.Replace(sql, "CREATE MATERIALIZED VIEW", "CREATE MATERIALIZED VIEW IF NOT EXISTS", 1)
		}
	}
	return sql
}

func MakeDistributedDDL(sql, cluster string) string {
	if IsDistributedDDL(sql) {
		return ReplaceCluster(sql, cluster)
	}

	return strings.Replace(sql, "(", fmt.Sprintf(" ON CLUSTER `%v` (", cluster), 1)
}

func SetTargetDatabase(ddl string, sourceDB, targetDB string) string {
	switch {
	case strings.Contains(ddl, fmt.Sprintf("CREATE TABLE %v.", sourceDB)):
		ddl = strings.Replace(ddl, fmt.Sprintf("CREATE TABLE %v.", sourceDB), fmt.Sprintf("CREATE TABLE `%v`.", targetDB), 1)
	case strings.Contains(ddl, fmt.Sprintf("CREATE TABLE `%v`.", sourceDB)):
		ddl = strings.Replace(ddl, fmt.Sprintf("CREATE TABLE `%v`.", sourceDB), fmt.Sprintf("CREATE TABLE `%v`.", targetDB), 1)
	}
	return ddl
}

func SetAltName(ddl string, targetDB string, names map[string]string) string {
	for from, to := range names {
		switch {
		case strings.Contains(ddl, fmt.Sprintf("CREATE TABLE %v.%v", targetDB, from)):
			ddl = strings.Replace(ddl, fmt.Sprintf("CREATE TABLE %v.%v", targetDB, from), fmt.Sprintf("CREATE TABLE `%v`.`%v`", targetDB, to), 1)
		case strings.Contains(ddl, fmt.Sprintf("CREATE TABLE `%v`.%v", targetDB, from)):
			ddl = strings.Replace(ddl, fmt.Sprintf("CREATE TABLE `%v`.%v", targetDB, from), fmt.Sprintf("CREATE TABLE `%v`.`%v`", targetDB, to), 1)
		case strings.Contains(ddl, fmt.Sprintf("CREATE TABLE %v.`%v`", targetDB, from)):
			ddl = strings.Replace(ddl, fmt.Sprintf("CREATE TABLE %v.`%v`", targetDB, from), fmt.Sprintf("CREATE TABLE `%v`.`%v`", targetDB, to), 1)
		case strings.Contains(ddl, fmt.Sprintf("CREATE TABLE `%v`.`%v`", targetDB, from)):
			ddl = strings.Replace(ddl, fmt.Sprintf("CREATE TABLE `%v`.`%v`", targetDB, from), fmt.Sprintf("CREATE TABLE `%v`.`%v`", targetDB, to), 1)
		}
	}
	return ddl
}
