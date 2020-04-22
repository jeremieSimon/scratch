package bytesort

import (
	"bytes"
	"encoding/binary"
)

type IntBasedComparableKey struct {
	ComparableLen uint8
	Comparable uint32
	ValueOffset uint64
	ValueLen uint32
}

func NewSimpleKey(comparable uint32, valueOffset uint64, valueLen uint32) *IntBasedComparableKey {
	return &IntBasedComparableKey{
		ComparableLen: 0,
		Comparable:    comparable,
		ValueOffset:   valueOffset,
		ValueLen:      valueLen,
	}
}

func (k *IntBasedComparableKey) Unmarshal(bts []byte) {
	k.Comparable = binary.BigEndian.Uint32(bts[1:5])
	k.ValueOffset = binary.BigEndian.Uint64(bts[5:13])
	k.ValueLen = binary.BigEndian.Uint32(bts[13:17])
}

func (k *IntBasedComparableKey) Marshal() []byte {
	buf := new(bytes.Buffer)

	b := make([]byte, 4)
	b[0] = byte(k.Comparable >> 24)
	b[1] = byte(k.Comparable >> 16)
	b[2] = byte(k.Comparable >> 8)
	b[3] = byte(k.Comparable)

	if b[0] != 0 {
		k.ComparableLen = 4
	} else if b[1] != 0 {
		k.ComparableLen = 3
	}  else if b[2] != 0 {
		k.ComparableLen = 2
	} else if b[3] != 0 {
		k.ComparableLen = 1
	}

	binary.Write(buf, binary.BigEndian, k.ComparableLen)
	buf.Write(b)
	binary.Write(buf, binary.BigEndian, k.ValueOffset)
	binary.Write(buf, binary.BigEndian, k.ValueLen)
	return buf.Bytes()
}

func (k IntBasedComparableKey) SmallerThan(left, right []byte) bool {
	return k.Compare(left, right) < 0
}

func (k IntBasedComparableKey) Compare(left, right []byte) int {
	if left[0] > right[0] {
		return 1
	} else if left[0] < right[0] {
		return -1
	}
	for i := 1; i < 9; i++ {
		if left[i] < right[i] {
			return -1
		} else if left[i] > right[i] {
			return 1
		}
	}
	return 0
}
