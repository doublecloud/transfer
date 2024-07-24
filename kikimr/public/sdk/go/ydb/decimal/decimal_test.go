package decimal

import (
	"encoding/binary"
	"testing"
)

func TestFromBytes(t *testing.T) {
	for _, test := range []struct {
		name      string
		bts       []byte
		precision uint32
		scale     uint32
	}{
		{
			bts:       uint128(0xffffffffffffffff, 0xffffffffffffffff),
			precision: 22,
			scale:     9,
		},
		{
			bts:       uint128(0xffffffffffffffff, 0),
			precision: 22,
			scale:     9,
		},
		{
			bts:       uint128(0x4000000000000000, 0),
			precision: 22,
			scale:     9,
		},
		{
			bts:       uint128(0x8000000000000000, 0),
			precision: 22,
			scale:     9,
		},
		{
			bts:       uint128s(1000000000),
			precision: 22,
			scale:     9,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			x := FromBytes(test.bts, test.precision, test.scale)
			p := Append(nil, x)
			y := FromBytes(p, test.precision, test.scale)
			if x.Cmp(y) != 0 {
				t.Errorf(
					"parsed bytes serialized to different value: %v; want %v",
					x, y,
				)
			}
			t.Logf(
				"%s %s",
				Format(x, test.precision, test.scale),
				Format(y, test.precision, test.scale),
			)
		})
	}
}

func uint128(hi, lo uint64) []byte {
	p := make([]byte, 16)
	binary.BigEndian.PutUint64(p[:8], hi)
	binary.BigEndian.PutUint64(p[8:], lo)
	return p
}

func uint128s(lo uint64) []byte {
	return uint128(0, lo)
}
