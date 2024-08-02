package httpyav

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"golang.org/x/crypto/ssh"
)

// computeRSASignature returns request RSA signature
func computeRSASignature(serializedRequest string, key *rsa.PrivateKey) (string, error) {
	// make SHA256 hash of serialized request and get hex representation
	hash256Sum := sha256.Sum256([]byte(serializedRequest))
	hexSum := make([]byte, len(hash256Sum)*2)
	_ = hex.Encode(hexSum, hash256Sum[:])

	// make SHA1 of hex representation
	hashSum := sha1.Sum(hexSum)

	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA1, hashSum[:])
	if err != nil {
		return "", err
	}

	// see: https://github.com/doublecloud/tross/arc/trunk/arcadia/contrib/python/paramiko/paramiko/rsakey.py#L123-126
	const signaturePrefix = "ssh-rsa"

	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.BigEndian, uint32(len(signaturePrefix)))
	_ = binary.Write(buf, binary.BigEndian, []byte(signaturePrefix))
	_ = binary.Write(buf, binary.BigEndian, uint32(len(sig)))
	_ = binary.Write(buf, binary.BigEndian, sig)

	return base64.URLEncoding.EncodeToString(buf.Bytes()), nil
}

// defaultHash returns sha256 representation
// see: https://github.com/doublecloud/tross/arc/trunk/arcadia/passport/infra/daemons/vault/api/utils/secrets.py?rev=r9154062#L29
func defaultHash(value string) []byte {
	hash256Sum := sha256.Sum256([]byte(value))
	hexSum := make([]byte, len(hash256Sum)*2)
	_ = hex.Encode(hexSum, hash256Sum[:])
	return hexSum
}

// computeRSASignatureByAgent returns request RSA signature
func computeRSASignatureByAgent(serializedRequest string, signer ssh.Signer) (string, error) {
	hashSum := defaultHash(serializedRequest)
	signature, err := signer.Sign(rand.Reader, hashSum)
	if err != nil {
		return "", fmt.Errorf("cannot sign request with RSA key: %w", err)
	}
	sig := signature.Blob

	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.BigEndian, uint32(len(signature.Format)))
	_ = binary.Write(buf, binary.BigEndian, []byte(signature.Format))
	_ = binary.Write(buf, binary.BigEndian, uint32(len(sig)))
	_ = binary.Write(buf, binary.BigEndian, sig)

	return base64.URLEncoding.EncodeToString(buf.Bytes()), nil
}
