package ydb

import (
	"bytes"
	"errors"
	"strconv"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	grpccodes "google.golang.org/grpc/codes"
)

type Issue struct {
	Message  string
	Code     uint32
	Severity uint32
}

var ErrOperationNotReady = errors.New("operation is not ready yet")

type IssueIterator []*Ydb_Issue.IssueMessage

func (it IssueIterator) Len() int {
	return len(it)
}

func (it IssueIterator) Get(i int) (issue Issue, nested IssueIterator) {
	x := it[i]
	if xs := x.Issues; len(xs) > 0 {
		nested = IssueIterator(xs)
	}
	return Issue{
		Message:  x.GetMessage(),
		Code:     x.GetIssueCode(),
		Severity: x.GetSeverity(),
	}, nested
}

type TransportError struct {
	Reason TransportErrorCode

	message string
	err     error
}

func (t *TransportError) Error() string {
	s := "ydb: transport error: " + t.Reason.String()
	if t.message != "" {
		s += ": " + t.message
	}
	return s
}

func (t *TransportError) Unwrap() error {
	return t.err
}

// OpError reports about operation fail.
type OpError struct {
	Reason StatusCode

	issues []*Ydb_Issue.IssueMessage
}

func (e *OpError) Issues() IssueIterator {
	return IssueIterator(e.issues)
}

func (e *OpError) Error() string {
	if len(e.issues) == 0 {
		return e.Reason.String()
	}
	var buf bytes.Buffer
	buf.WriteString("ydb: operation error: ")
	buf.WriteString(e.Reason.String())
	if len(e.issues) > 0 {
		buf.WriteByte(':')
		dumpIssues(&buf, e.issues)
	}
	return buf.String()
}

func iterateIssues(issues []*Ydb_Issue.IssueMessage, it func(Issue)) {
	for _, x := range issues {
		it(Issue{
			Message:  x.GetMessage(),
			Code:     x.GetIssueCode(),
			Severity: x.GetSeverity(),
		})
	}
}

func dumpIssues(buf *bytes.Buffer, ms []*Ydb_Issue.IssueMessage) {
	if len(ms) == 0 {
		return
	}
	buf.WriteByte(' ')
	buf.WriteByte('[')
	defer buf.WriteByte(']')
	for _, m := range ms {
		buf.WriteByte('{')
		if code := m.GetIssueCode(); code != 0 {
			buf.WriteByte('#')
			buf.WriteString(strconv.Itoa(int(code)))
			buf.WriteByte(' ')
		}
		buf.WriteString(strings.TrimSuffix(m.GetMessage(), "."))
		dumpIssues(buf, m.Issues)
		buf.WriteByte('}')
	}
}

// StatusCode reports unsuccessful operation status code.
type StatusCode int32

func (e StatusCode) String() string {
	return Ydb.StatusIds_StatusCode_name[int32(e)]
}

func (e StatusCode) operationCompleted() OperationCompleted {
	switch e {
	case
		StatusAborted,
		StatusUnavailable,
		StatusOverloaded,
		StatusBadSession,
		StatusSessionBusy,
		StatusNotFound:
		return OperationCompletedFalse
	case
		StatusUndetermined:
		return OperationCompletedUndefined
	default:
		return OperationCompletedTrue
	}
}

func (e StatusCode) backoffType() BackoffType {
	switch e {
	case
		StatusOverloaded:
		return BackoffTypeSlowBackoff
	case
		StatusAborted,
		StatusUnavailable,
		StatusCancelled,
		StatusSessionBusy,
		StatusUndetermined:
		return BackoffTypeFastBackoff
	default:
		return BackoffTypeNoBackoff
	}
}

func (e StatusCode) mustDeleteSession() bool {
	switch e {
	case
		StatusBadSession,
		StatusSessionExpired,
		StatusSessionBusy:
		return true
	default:
		return false
	}
}

// Errors describing unsusccessful operation status.
const (
	StatusUnknownStatus      = StatusCode(Ydb.StatusIds_STATUS_CODE_UNSPECIFIED)
	StatusBadRequest         = StatusCode(Ydb.StatusIds_BAD_REQUEST)
	StatusUnauthorized       = StatusCode(Ydb.StatusIds_UNAUTHORIZED)
	StatusInternalError      = StatusCode(Ydb.StatusIds_INTERNAL_ERROR)
	StatusAborted            = StatusCode(Ydb.StatusIds_ABORTED)
	StatusUnavailable        = StatusCode(Ydb.StatusIds_UNAVAILABLE)
	StatusOverloaded         = StatusCode(Ydb.StatusIds_OVERLOADED)
	StatusSchemeError        = StatusCode(Ydb.StatusIds_SCHEME_ERROR)
	StatusGenericError       = StatusCode(Ydb.StatusIds_GENERIC_ERROR)
	StatusTimeout            = StatusCode(Ydb.StatusIds_TIMEOUT)
	StatusBadSession         = StatusCode(Ydb.StatusIds_BAD_SESSION)
	StatusPreconditionFailed = StatusCode(Ydb.StatusIds_PRECONDITION_FAILED)
	StatusAlreadyExists      = StatusCode(Ydb.StatusIds_ALREADY_EXISTS)
	StatusNotFound           = StatusCode(Ydb.StatusIds_NOT_FOUND)
	StatusSessionExpired     = StatusCode(Ydb.StatusIds_SESSION_EXPIRED)
	StatusCancelled          = StatusCode(Ydb.StatusIds_CANCELLED)
	StatusUndetermined       = StatusCode(Ydb.StatusIds_UNDETERMINED)
	StatusUnsupported        = StatusCode(Ydb.StatusIds_UNSUPPORTED)
	StatusSessionBusy        = StatusCode(Ydb.StatusIds_SESSION_BUSY)
)

func statusCode(s Ydb.StatusIds_StatusCode) StatusCode {
	switch s {
	case Ydb.StatusIds_BAD_REQUEST:
		return StatusBadRequest
	case Ydb.StatusIds_UNAUTHORIZED:
		return StatusUnauthorized
	case Ydb.StatusIds_INTERNAL_ERROR:
		return StatusInternalError
	case Ydb.StatusIds_ABORTED:
		return StatusAborted
	case Ydb.StatusIds_UNAVAILABLE:
		return StatusUnavailable
	case Ydb.StatusIds_OVERLOADED:
		return StatusOverloaded
	case Ydb.StatusIds_SCHEME_ERROR:
		return StatusSchemeError
	case Ydb.StatusIds_GENERIC_ERROR:
		return StatusGenericError
	case Ydb.StatusIds_TIMEOUT:
		return StatusTimeout
	case Ydb.StatusIds_BAD_SESSION:
		return StatusBadSession
	case Ydb.StatusIds_PRECONDITION_FAILED:
		return StatusPreconditionFailed
	case Ydb.StatusIds_ALREADY_EXISTS:
		return StatusAlreadyExists
	case Ydb.StatusIds_NOT_FOUND:
		return StatusNotFound
	case Ydb.StatusIds_SESSION_EXPIRED:
		return StatusSessionExpired
	case Ydb.StatusIds_CANCELLED:
		return StatusCancelled
	case Ydb.StatusIds_UNDETERMINED:
		return StatusUndetermined
	case Ydb.StatusIds_UNSUPPORTED:
		return StatusUnsupported
	case Ydb.StatusIds_SESSION_BUSY:
		return StatusSessionBusy
	default:
		return StatusUnknownStatus
	}
}

type TransportErrorCode int32

func (t TransportErrorCode) String() string {
	return transportErrorString(t)
}

func (t TransportErrorCode) operationCompleted() OperationCompleted {
	switch t {
	case
		TransportErrorAborted,
		TransportErrorResourceExhausted:
		return OperationCompletedFalse
	case
		TransportErrorInternal,
		TransportErrorCanceled,
		TransportErrorUnavailable:
		return OperationCompletedUndefined
	default:
		return OperationCompletedTrue
	}
}

func (t TransportErrorCode) backoffType() BackoffType {
	switch t {
	case
		TransportErrorInternal,
		TransportErrorCanceled,
		TransportErrorUnavailable:
		return BackoffTypeFastBackoff
	case
		TransportErrorResourceExhausted:
		return BackoffTypeSlowBackoff
	default:
		return BackoffTypeNoBackoff
	}
}

func (t TransportErrorCode) mustDeleteSession() bool {
	switch t {
	case
		TransportErrorResourceExhausted,
		TransportErrorOutOfRange:
		return false
	default:
		return true
	}
}

const (
	TransportErrorUnknownCode TransportErrorCode = iota
	TransportErrorCanceled
	TransportErrorUnknown
	TransportErrorInvalidArgument
	TransportErrorDeadlineExceeded
	TransportErrorNotFound
	TransportErrorAlreadyExists
	TransportErrorPermissionDenied
	TransportErrorResourceExhausted
	TransportErrorFailedPrecondition
	TransportErrorAborted
	TransportErrorOutOfRange
	TransportErrorUnimplemented
	TransportErrorInternal
	TransportErrorUnavailable
	TransportErrorDataLoss
	TransportErrorUnauthenticated
)

var grpcCodesToTransportError = [...]TransportErrorCode{
	grpccodes.Canceled:           TransportErrorCanceled,
	grpccodes.Unknown:            TransportErrorUnknown,
	grpccodes.InvalidArgument:    TransportErrorInvalidArgument,
	grpccodes.DeadlineExceeded:   TransportErrorDeadlineExceeded,
	grpccodes.NotFound:           TransportErrorNotFound,
	grpccodes.AlreadyExists:      TransportErrorAlreadyExists,
	grpccodes.PermissionDenied:   TransportErrorPermissionDenied,
	grpccodes.ResourceExhausted:  TransportErrorResourceExhausted,
	grpccodes.FailedPrecondition: TransportErrorFailedPrecondition,
	grpccodes.Aborted:            TransportErrorAborted,
	grpccodes.OutOfRange:         TransportErrorOutOfRange,
	grpccodes.Unimplemented:      TransportErrorUnimplemented,
	grpccodes.Internal:           TransportErrorInternal,
	grpccodes.Unavailable:        TransportErrorUnavailable,
	grpccodes.DataLoss:           TransportErrorDataLoss,
	grpccodes.Unauthenticated:    TransportErrorUnauthenticated,
}

func transportErrorCode(c grpccodes.Code) TransportErrorCode {
	if int(c) < len(grpcCodesToTransportError) {
		return grpcCodesToTransportError[c]
	}
	return TransportErrorUnknownCode
}

var transportErrorToString = [...]string{
	TransportErrorCanceled:           "canceled",
	TransportErrorUnknown:            "unknown",
	TransportErrorInvalidArgument:    "invalid argument",
	TransportErrorDeadlineExceeded:   "deadline exceeded",
	TransportErrorNotFound:           "not found",
	TransportErrorAlreadyExists:      "already exists",
	TransportErrorPermissionDenied:   "permission denied",
	TransportErrorResourceExhausted:  "resource exhausted",
	TransportErrorFailedPrecondition: "failed precondition",
	TransportErrorAborted:            "aborted",
	TransportErrorOutOfRange:         "out of range",
	TransportErrorUnimplemented:      "unimplemented",
	TransportErrorInternal:           "internal",
	TransportErrorUnavailable:        "unavailable",
	TransportErrorDataLoss:           "data loss",
	TransportErrorUnauthenticated:    "unauthenticated",
}

func transportErrorString(t TransportErrorCode) string {
	if int(t) < len(transportErrorToString) {
		return transportErrorToString[t]
	}
	return "unknown code"
}
