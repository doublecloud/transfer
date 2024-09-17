package postgres

import (
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/errors/coded"
	"github.com/jackc/pgconn"
)

var (
	NoPrimaryKeyCode = coded.Register("postgres", "no_primary_key")
)

type PgErrorCode string

// PostgreSQL error codes from https://www.postgresql.org/docs/12/errcodes-appendix.html
const (
	ErrcUniqueViolation              PgErrorCode = "23505"
	ErrcWrongObjectType              PgErrorCode = "42809"
	ErrcRelationDoesNotExists        PgErrorCode = "42P01"
	ErrcSchemaDoesNotExists          PgErrorCode = "3F000"
	ErrcObjectNotInPrerequisiteState PgErrorCode = "55000"
	ErrcInvalidPassword              PgErrorCode = "28P01"
	ErrcInvalidAuthSpec              PgErrorCode = "28000"
)

func IsPgError(err error, code PgErrorCode) bool {
	var pgErr pgconn.PgError
	pgErrPtr := &pgErr
	if !xerrors.As(err, &pgErrPtr) {
		return false
	}
	return pgErrPtr.Code == string(code)
}

func IsPKeyCheckError(err error) bool {
	var codederr coded.CodedError
	if xerrors.As(err, &codederr) {
		return codederr.Code() == NoPrimaryKeyCode
	}
	return false
}
