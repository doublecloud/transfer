package table

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"hash"
)

type queryHasher struct {
	sha hash.Hash
	buf bytes.Buffer
	bts []byte
}

type queryHash [4]uint64

func (h *queryHasher) hash(s string) (r queryHash) {
	if h.sha == nil {
		h.sha = sha256.New()
	}

	h.buf.WriteString(s)
	_, _ = h.buf.WriteTo(h.sha)

	bts := h.sha.Sum(h.bts[:0])

	r[0] = binary.LittleEndian.Uint64(bts[0:8])
	r[1] = binary.LittleEndian.Uint64(bts[8:16])
	r[2] = binary.LittleEndian.Uint64(bts[16:24])
	r[3] = binary.LittleEndian.Uint64(bts[24:32])

	h.bts = bts
	h.sha.Reset()

	return
}
