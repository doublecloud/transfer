package postgres

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/middlewares"
	sink_factory "github.com/doublecloud/transfer/pkg/sink"
	"github.com/doublecloud/transfer/pkg/util"
	"github.com/jackc/pgx/v4"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/exp/slices"
)

type pgDumpItem struct {
	Name   string
	Typ    string
	Owner  string
	Body   string
	Schema string
}

var typesExistsQuery = `SELECT EXISTS (
SELECT t.typname as type
FROM pg_type t
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))
AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
AND n.nspname NOT IN ('pg_catalog', 'information_schema')
);`

func (i *pgDumpItem) TableDescription() (*abstract.TableDescription, error) {
	scanner := bufio.NewScanner(bytes.NewReader([]byte(i.Body)))
	for scanner.Scan() {
		row := scanner.Text()
		if strings.HasPrefix(row, "CREATE TABLE ") {
			parts := strings.Split(row, " ")
			if len(parts) > 3 {
				fqtn := strings.Split(parts[2], ".")
				if len(fqtn) > 1 {
					schema := strings.ReplaceAll(fqtn[0], "\"", "")
					name := strings.ReplaceAll(fqtn[1], "\"", "")
					return &abstract.TableDescription{
						Name:   name,
						Schema: schema,
						Filter: "",
						EtaRow: 0,
						Offset: 0,
					}, nil
				}
			}
		}
	}

	return nil, xerrors.New("Not found `CREATE TABLE` line")
}

func ApplyCommands(commands []*pgDumpItem, transfer server.Transfer, registry metrics.Registry, types ...string) error {
	if _, ok := transfer.Dst.(*PgDestination); !ok {
		return nil
	}
	sink, err := sink_factory.MakeAsyncSink(&transfer, logger.Log, registry, coordinator.NewFakeClient(), middlewares.MakeConfig(middlewares.WithNoData))
	if err != nil {
		return err
	}
	defer sink.Close()
	allowedType := map[string]bool{}
	for _, typ := range types {
		allowedType[typ] = true
	}
	// we need to move schema always, since all items somewhat bounded to it
	allowedType["SCHEMA"] = true
	for _, command := range commands {
		if !allowedType[command.Typ] {
			continue
		}
		logger.Log.Infof("Try to apply PostgreSQL DDL of type '%v', name '%v'.'%v'", command.Typ, command.Schema, command.Name)
		if err := <-sink.AsyncPush([]abstract.ChangeItem{{
			CommitTime:   uint64(time.Now().UnixNano()),
			Kind:         abstract.PgDDLKind,
			ColumnValues: []interface{}{command.Body},
		}}); err != nil {
			if isAlreadyExistsError(err) {
				logger.Log.Warnf("Object(type '%v', name '%v'.'%v') already exists or is already performed", command.Typ, command.Schema, command.Name)
				continue
			}
			if command.Typ == "PRIMARY_KEY" {
				if strings.Contains(err.Error(), "multiple primary keys for table") {
					logger.Log.Warn(
						fmt.Sprintf("Multiple primary keys for '%v', name '%v'.'%v'", command.Typ, command.Schema, command.Name),
						log.Error(err))
					continue
				}
			}
			logger.Log.Error(
				fmt.Sprintf("Unable to apply DDL of type '%v', name '%v'.'%v'", command.Typ, command.Schema, command.Name),
				log.String("query", command.Body),
				log.Error(err),
			)
			return xerrors.Errorf(
				"Unable to apply DDL of type '%v', name '%v'.'%v', error: %w",
				command.Typ, command.Schema, command.Name, err)
		}
	}
	return nil
}

func isAlreadyExistsError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "already exists") || strings.Contains(msg, "is already a partition")
}

func formatFqtn(in string) (string, error) {
	tableID, err := abstract.NewTableIDFromStringPg(in, false)
	if err != nil {
		return "", xerrors.Errorf("failed to parse: %w", err)
	}
	return tableID.Fqtn(), nil
}

func PostgresDumpConnString(src *PgSource) (string, server.SecretString, error) {

	config, err := GetConnParamsFromSrc(logger.Log, src)
	if err != nil {
		return "", "", err
	}
	logger.Log.Infof("Getting dump conn string for master host '%s'", config.Host)

	if config.HasTLS {
		customCaPath := "./customRootCA.crt"
		if err := os.WriteFile(customCaPath, []byte(config.CACertificates), 0o664); err != nil {
			return "", "", xerrors.Errorf("failed to write a custom SSL root certificate into a local file: %w", err)
		}
		return fmt.Sprintf("host=%v port=%v dbname=%v user=%v sslmode=verify-full sslrootcert=%v", config.Host, config.Port, config.Database, config.User, customCaPath), config.Password, nil
	} else {
		return fmt.Sprintf("host=%v port=%v dbname=%v user=%v", config.Host, config.Port, config.Database, config.User), config.Password, nil
	}
}

func pgDumpSchemaArgs(src *PgSource, seqsIncluded []abstract.TableID, seqsExcluded []abstract.TableID) ([]string, error) {
	args := make([]string, 0)
	args = append(args,
		"--no-publications",
		"--no-subscriptions",
		"--format=plain",
		"--no-owner",
		"--schema-only",
	)
	initialArgsCount := len(args)

	if len(src.DBTables) > 0 {
		for _, t := range src.DBTables {
			if len(t) == 0 {
				// TM-1964
				continue
			}
			arg, err := formatFqtn(t)
			if err != nil {
				return nil, xerrors.Errorf("failed to format directive '%s': %w", t, err)
			}
			args = append(args, "-t", arg)
		}
		for _, t := range src.AuxTables() {
			args = append(args, "-t", t)
		}
		for _, seq := range seqsIncluded {
			args = append(args, "-t", seq.Fqtn())
		}
	}

	if len(args) > initialArgsCount {
		return args, nil
	} // otherwise, all objects in the database are dumped

	for _, t := range src.ExcludeWithGlobals() {
		if len(t) == 0 {
			// TM-1964
			continue
		}
		arg, err := formatFqtn(t)
		if err != nil {
			return nil, xerrors.Errorf("failed to format directive '%s': %w", t, err)
		}
		args = append(args, "-T", arg)
	}
	for _, seq := range seqsExcluded {
		args = append(args, "-T", seq.Fqtn())
	}

	return args, nil
}

// dumpSequenceValues produces SEQUENCE SET pg_dump events which transmit the current state of all sequences in the given list from the source database
func dumpSequenceValues(ctx context.Context, conn *pgx.Conn, sequences []abstract.TableID) ([]*pgDumpItem, error) {
	result := make([]*pgDumpItem, 0)
	for _, seq := range sequences {
		lastValue, isCalled, err := GetCurrentStateOfSequence(ctx, conn, seq)
		if err != nil {
			return nil, xerrors.Errorf("failed to get current state of SEQUENCE %s: %w", seq.String(), err)
		}
		seqItem := &pgDumpItem{
			Name:   seq.Name,
			Typ:    string(SequenceSet),
			Owner:  "",
			Body:   fmt.Sprintf("SELECT pg_catalog.setval('%s', %d, %t);", seq.Fqtn(), lastValue, isCalled),
			Schema: seq.Namespace,
		}
		result = append(result, seqItem)
	}
	return result, nil
}

// sourceInPgPg returns a non-nil object only for homogenous PG-PG transfers
func sourceInPgPg(transfer *server.Transfer) *PgSource {
	var src *PgSource
	var srcIsPG bool
	var dstIsPG bool
	src, srcIsPG = transfer.Src.(*PgSource)
	_, dstIsPG = transfer.Dst.(*PgDestination)
	if !(srcIsPG && dstIsPG) {
		return nil
	}
	return src
}

// ExtractPgDumpSchema returns the dump ONLY for homogenous PG-PG transfers. It also logs its actions
func ExtractPgDumpSchema(transfer *server.Transfer) ([]*pgDumpItem, error) {
	src := sourceInPgPg(transfer)
	if src == nil {
		return nil, nil
	}

	logger.Log.Info("Schema will be extracted by pg_dump for a PostgreSQL-PostgreSQL transfer")
	pgdump, err := loadPgDumpSchema(context.Background(), src, transfer)
	if err != nil {
		return nil, xerrors.Errorf("failed to extract schema from the source PostgreSQL by pg_dump: %w", err)
	}
	logger.Log.Info("Successfully extracted schema from PostgreSQL source by pg_dump", log.Int("len", len(pgdump)))
	return pgdump, nil
}

// ApplyPgDumpPreSteps takes the given dump and applies pre-steps defined in transfer source ONLY for homogenous PG-PG transfers. It also logs its actions
func ApplyPgDumpPreSteps(pgdump []*pgDumpItem, transfer *server.Transfer, registry metrics.Registry) error {
	if len(pgdump) == 0 {
		return nil
	}
	src := sourceInPgPg(transfer)
	if src == nil {
		return nil
	}

	if err := ApplyCommands(pgdump, *transfer, registry, src.PreSteps.List()...); err != nil {
		return xerrors.Errorf("failed to apply schema pre-steps (%v) in the destination PostgreSQL: %w", src.PreSteps.List(), err)
	}
	logger.Log.Info("Successfully applied schema pre-steps in the destination PostgreSQL", log.Array("steps", src.PreSteps.List()))
	return nil
}

// ApplyPgDumpPostSteps takes the given dump and applies post-steps defined in transfer source ONLY for homogenous PG-PG transfers. It also logs its actions
func ApplyPgDumpPostSteps(pgdump []*pgDumpItem, transfer *server.Transfer, registry metrics.Registry) error {
	if len(pgdump) == 0 {
		return nil
	}
	src := sourceInPgPg(transfer)
	if src == nil {
		return nil
	}

	if err := ApplyCommands(pgdump, *transfer, registry, src.PostSteps.List()...); err != nil {
		return xerrors.Errorf("failed to apply schema post-steps (%v) in the destination PostgreSQL: %w", src.PostSteps.List(), err)
	}
	logger.Log.Info("Successfully applied schema post-steps in the destination PostgreSQL", log.Array("steps", src.PostSteps.List()))
	return nil
}

// loadPgDumpSchema actually loads the schema from PostgreSQL source using a storage constructed in-place
func loadPgDumpSchema(ctx context.Context, src *PgSource, transfer *server.Transfer) ([]*pgDumpItem, error) {
	storage, err := NewStorage(src.ToStorageParams(transfer))
	if err != nil {
		return nil, xerrors.Errorf("failed to create a PostgreSQL Storage object: %w", err)
	}
	defer storage.Close()

	tx, err := storage.Conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite, DeferrableMode: pgx.NotDeferrable})
	if err != nil {
		return nil, xerrors.Errorf("failed to BEGIN transaction: %w", err)
	}
	defer func() {
		// we always ROLLBACK the schema retrieval transaction, it should not change anything in the source database even though it is read-write. So no rollbacks usage is necessary
		err := tx.Rollback(ctx)
		if err != nil {
			logger.Log.Warn("failed to ROLLBACK transaction", log.Error(err))
		}
	}()

	connString, secretPass, err := PostgresDumpConnString(src)
	if err != nil {
		return nil, xerrors.Errorf("failed to build PostgreSQL connection string: %w", err)
	}

	seqs, err := listAllSequences(ctx, src, tx.Conn())
	if err != nil {
		return nil, xerrors.Errorf("failed to list all SEQUENCEs: %w", err)
	}
	seqsIncluded, seqsExcluded := filterSequences(seqs, abstract.NewIntersectionIncludeable(src, transfer))

	result, err := dumpUserDefinedTypes(ctx, connString, secretPass, src)
	if err != nil {
		return nil, err
	}

	pgDumpArgs, err := pgDumpSchemaArgs(src, seqsIncluded, seqsExcluded)
	if err != nil {
		return nil, xerrors.Errorf("failed to compose arguments for pg_dump: %w", err)
	}
	dump, err := execPgDump(src.PgDumpCommand, connString, secretPass, pgDumpArgs)
	result = append(result, dump...)
	if err != nil {
		return nil, xerrors.Errorf("failed to execute pg_dump to get schema: %w", err)
	}

	if (src.PreSteps.SequenceSet == nil || *src.PreSteps.SequenceSet) || (src.PostSteps.SequenceSet == nil || *src.PostSteps.SequenceSet) {
		sequenceValuesDump, err := dumpSequenceValues(ctx, tx.Conn(), seqsIncluded)
		if err != nil {
			return nil, xerrors.Errorf("failed to dump current SEQUENCE values: %w", err)
		}
		result = append(result, sequenceValuesDump...)
	}

	return result, nil
}

// listAllSequences constructs a pg Storage in-place and obtains all (accessible) SEQUENCEs
func listAllSequences(ctx context.Context, src *PgSource, conn *pgx.Conn) (SequenceMap, error) {
	if !src.PreSteps.Sequence && !src.PostSteps.Sequence {
		return make(SequenceMap), nil
	}

	return ListSequencesWithDependants(ctx, conn, src.KeeperSchema)
}

// filterSequences separates the given sequences into included and excluded ones by applying the given filter
func filterSequences(sequences SequenceMap, filter abstract.Includeable) (included []abstract.TableID, excluded []abstract.TableID) {
	for _, sequenceInfo := range sequences {
		sequenceIncluded := false
		if len(sequenceInfo.DependentTables) == 0 {
			// special case for a SEQUENCE which is not used by any table
			sequenceIncluded = filter.Include(abstract.NonExistentTableID)
		}
		for _, table := range sequenceInfo.DependentTables {
			if filter.Include(table) {
				logger.Log.Info("Sequence included", log.String("sequence", sequenceInfo.SequenceID.Fqtn()), log.String("table", table.Fqtn()))
				sequenceIncluded = true
				break
			}
		}
		if sequenceIncluded {
			included = append(included, sequenceInfo.SequenceID)
		} else {
			excluded = append(excluded, sequenceInfo.SequenceID)
		}
	}
	slices.SortStableFunc(included, abstract.TableID.Less)
	slices.SortStableFunc(excluded, abstract.TableID.Less)
	return included, excluded
}

func dumpUserDefinedTypes(ctx context.Context, connString string, connPass server.SecretString, src *PgSource) ([]*pgDumpItem, error) {
	if len(src.DBTables) == 0 || (!src.PreSteps.Type && !src.PostSteps.Type) {
		return nil, nil
	}

	if containsUserDefinedTypes, err := pgContainsUserDefinedTypes(ctx, src); !containsUserDefinedTypes || err != nil {
		return nil, err
	}

	tablesSchemas := util.NewSet[string]()
	for _, t := range src.DBTables {
		tableID, err := abstract.NewTableIDFromStringPg(t, false)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse from string: %w", err)
		}
		tablesSchemas.Add(tableID.Namespace)
	}

	args := []string{
		"--no-publications",
		"--no-subscriptions",
		"--format=plain",
		"--no-owner",
		"--schema-only",
		"-T",
		"*.*",
	}

	result := make([]*pgDumpItem, 0)
	dump, err := execPgDump(src.PgDumpCommand, connString, connPass, args)
	if err != nil {
		return nil, xerrors.Errorf("failed to execute pg_dump to get user-defined types: %w", err)
	}

	for _, d := range dump {
		if tablesSchemas.Contains(d.Schema) && d.Typ == "TYPE" {
			result = append(result, d)
		}
	}

	return result, nil
}

func execPgDump(pgDump []string, connString string, password server.SecretString, args []string) ([]*pgDumpItem, error) {
	if len(pgDump) == 0 {
		pgDump = []string{"pg_dump"}
	}

	commandArgs := []string{}
	if len(pgDump) > 1 {
		commandArgs = append(commandArgs, pgDump[1:]...)
	}
	commandArgs = append(commandArgs, connString)
	commandArgs = append(commandArgs, args...)
	command := exec.Command(pgDump[0], commandArgs...)
	if password != "" {
		command.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", password))
	}
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr

	logger.Log.Info("Run pg_dump", log.String("path", command.Path), log.Strings("args", command.Args))
	if err := command.Run(); err != nil {
		stderrBytes := stderr.Bytes()
		if bytes.Contains(stderrBytes, []byte("permission denied")) {
			// TM-1650: permission error should be fatal
			err = abstract.NewFatalError(err)
		}
		return nil, xerrors.Errorf("failed to execute pg_dump. STDERR:\n%s\nerror: %w", string(truncate(string(stderrBytes), 2000)), err)
	}
	pgDumpOut := parsePgDumpOut(&stdout)
	logPgDumpOut(pgDumpOut)
	return pgDumpOut, nil
}

func pgContainsUserDefinedTypes(ctx context.Context, src *PgSource) (bool, error) {
	var result bool
	conn, err := MakeConnPoolFromSrc(src, logger.Log)
	if err != nil {
		return false, xerrors.Errorf("failed to create a connection pool: %w", err)
	}
	defer conn.Close()

	checkType := func() error {
		err := conn.QueryRow(ctx, typesExistsQuery).Scan(&result)
		if err != nil {
			return xerrors.Errorf("failed to check user-defined types existence: %w", err)
		}
		return nil
	}
	err = backoff.Retry(checkType, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3))
	if err != nil {
		return false, err
	}
	return result, nil
}

func isPgDumpItemValid(item *pgDumpItem) bool {
	return item != nil && item.Typ != ""
}

func parsePgDumpOut(out io.Reader) []*pgDumpItem {
	var res []*pgDumpItem
	scanner := bufio.NewScanner(out)
	var current *pgDumpItem
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "--") {
			// At start of section with dump item meta
			if isPgDumpItemValid(current) {
				res = append(res, current)
			}
			current = new(pgDumpItem)
			if !scanner.Scan() {
				return res
			}
			nameLine := scanner.Text()
			nameLine = strings.ReplaceAll(nameLine, "--", "")
			parts := strings.Split(nameLine, ";")
			for _, p := range parts {
				kv := strings.Split(p, ":")
				switch strings.TrimSpace(kv[0]) {
				case "Name":
					current.Name = strings.TrimSpace(kv[1])
				case "Type":
					current.Typ = strings.ReplaceAll(strings.TrimSpace(kv[1]), " ", "_")
				case "Schema":
					current.Schema = strings.TrimSpace(kv[1])
				case "Owner":
					current.Owner = strings.TrimSpace(kv[1])
				}
			}
			_ = scanner.Scan()
		}
		if len(line) == 0 {
			continue
		}
		if current != nil {
			current.Body = current.Body + "\n" + line
			if current.Typ == "CONSTRAINT" && strings.Contains(line, "PRIMARY KEY") {
				current.Typ = "PRIMARY_KEY"
			}
		}
	}
	if isPgDumpItemValid(current) {
		res = append(res, current)
	}
	stat := map[string]int{}
	for _, item := range res {
		stat[item.Typ]++
	}
	for k, count := range stat {
		logger.Log.Infof("Found commands with type: %v - %v", k, count)
	}
	return res
}

func logPgDumpOut(items []*pgDumpItem) {
	for _, item := range items {
		jsonStr, _ := json.Marshal(*item)
		logger.Log.Infof("pg_dump item: %s", jsonStr)
	}
}
