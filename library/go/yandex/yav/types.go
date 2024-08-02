package yav

import (
	"strconv"
	"strings"
	"time"
)

const (
	StatusOK      = "ok"
	StatusError   = "error"
	StatusWarning = "warning"

	RoleOwner    = "OWNER"
	RoleReader   = "READER"
	RoleAppender = "APPENDER"
)

type Timestamp struct {
	time.Time
}

func (t *Timestamp) UnmarshalJSON(b []byte) (err error) {
	tsParts := strings.SplitN(string(b), ".", 2)

	var intpart, frac int64
	intpart, err = strconv.ParseInt(tsParts[0], 10, 64)
	if err != nil {
		return
	}

	if len(tsParts) > 1 {
		frac, err = strconv.ParseInt(tsParts[1], 10, 64)
		if err != nil {
			return
		}
	}

	t.Time = time.Unix(intpart, frac)
	return nil
}

func (t *Timestamp) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatInt(t.Unix(), 10)), nil
}
