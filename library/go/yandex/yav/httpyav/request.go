package httpyav

import (
	"crypto/rsa"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/doublecloud/tross/library/go/core/xerrors"
	"golang.org/x/crypto/ssh"
)

var timeNow = time.Now

// rsaKeySignRequest signs request using given RSA private key
func rsaKeySignRequest(req *http.Request, login string, key *rsa.PrivateKey) error {
	if key == nil {
		return nil
	}

	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		return xerrors.Errorf("create signer error: %w", err)
	}

	return rsaSignRequest(req, login, signer)
}

// rsaSignRequest signs request using ssh signer (e.g. agent key and so on)
func rsaSignRequest(req *http.Request, login string, signer ssh.Signer) error {
	ts := timeNow()

	serialized, err := serializeRequestV3(req, ts, login)
	if err != nil {
		return xerrors.Errorf("signing error: %w", err)
	}
	signature, err := computeRSASignatureByAgent(serialized, signer)
	if err != nil {
		return xerrors.Errorf("signing error: %w", err)
	}

	pk := signer.PublicKey()
	if strings.Contains(strings.ToLower(pk.Type()), "-cert") {
		pkb64 := base64.StdEncoding.EncodeToString(pk.Marshal())
		cert := fmt.Sprintf("%s %s", pk.Type(), pkb64)
		req.Header.Set("X-Ya-Rsa-Cert", cert)
	}

	req.Header.Set("X-Ya-Rsa-Signature", signature)
	req.Header.Set("X-Ya-Rsa-Login", login)
	req.Header.Set("X-Ya-Rsa-Timestamp", strconv.FormatInt(ts.Unix(), 10))
	return nil
}

// serializeRequestV3 serializes request
// see: https://github.com/doublecloud/tross/arc/trunk/arcadia/passport/infra/daemons/vault/api/views/base_view.py?rev=r9165057#L320
func serializeRequestV3(req *http.Request, ts time.Time, login string) (string, error) {
	if req.GetBody == nil {
		return "", xerrors.New("request func GetBody is not set")
	}

	body, err := req.GetBody()
	if err != nil {
		return "", xerrors.Errorf("request GetBody error: %w", err)
	}

	// see: https://github.com/doublecloud/tross/arc/trunk/arcadia/library/python/vault_client/vault_client/client.py?rev=r9165074#L281
	// otherwise nil will be encoded as 'null' not ''
	var payload []byte
	if body != nil {
		payload, err = io.ReadAll(body)
		if err != nil {
			return "", xerrors.Errorf("cannot read body for signing: %w", err)
		}
	}
	// see: https://github.com/doublecloud/tross/arc/trunk/arcadia/library/python/vault_client/vault_client/client.py?rev=r9165074#L274
	req.URL.ForceQuery = true
	return fmt.Sprintf("%s\n%s\n%s\n%d\n%s\n", strings.ToUpper(req.Method), req.URL.String(), payload,
		ts.Unix(), login), nil
}
