package coralogix

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/util"
)

// see: https://coralogix.com/docs/rest-api-bulk/

type Severity int

// 1 – Debug, 2 – Verbose, 3 – Info, 4 – Warn, 5 – Error, 6 – Critical
const (
	Debug    = Severity(1)
	Verbose  = Severity(2)
	Info     = Severity(3)
	Warn     = Severity(4)
	Error    = Severity(5)
	Critical = Severity(6)
)

type HTTPLogItem struct {
	ApplicationName string   `json:"applicationName"`
	SubsystemName   string   `json:"subsystemName"`
	ComputerName    string   `json:"computerName"`
	Timestamp       int64    `json:"timestamp,omitempty"`
	Severity        Severity `json:"severity"`
	Text            string   `json:"text"`
	Category        string   `json:"category"`
	ClassName       string   `json:"className"`
	MethodName      string   `json:"methodName"`
	ThreadID        string   `json:"threadId"`
	HiResTimestamp  string   `json:"hiResTimestamp,omitempty"`
}

var fatalCode = util.NewSet(403, 404)

func SubmitLogs(data []HTTPLogItem, domain, token string) error {
	payloadBytes, err := json.Marshal(data)
	if err != nil {
		return xerrors.Errorf("unable to marshal data: %w", err)
	}
	body := bytes.NewReader(payloadBytes)

	req, err := http.NewRequest("POST", fmt.Sprintf("https://ingress.%s/logs/v1/singles", domain), body)
	if err != nil {
		return xerrors.Errorf("unable to make request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return xerrors.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	if fatalCode.Contains(resp.StatusCode) {
		return abstract.NewFatalError(xerrors.Errorf("fatal error: %s", resp.Status))
	}
	resBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return xerrors.Errorf("submit failed: %s, unable to read response body: %w", resp.Status, err)
	}
	return xerrors.Errorf("submit failed: %s: %s", resp.Status, util.Sample(string(resBytes), 1024))
}
