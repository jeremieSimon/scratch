package mem

import (
	"encoding/binary"
	"github.com/dgryski/go-farm"
	"math"
	"sort"
)

type ImmutableMemTable struct {
	Keys    []uint32
	Offsets []int
	Values  []byte
}

func FromMap(m map[uint32][]byte) ImmutableMemTable {
	keys := make([]uint32, len(m))
	offsets := make([]int, len(m) + 1)
	values := make([]byte, 0)

	// 1. collect Keys
	i := 0
	for k := range m {
		keys[i] = k
		i += 1
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] <= keys[j]
	})

	// 2. build Offsets and Values
	for i, k := range keys {
		v := m[k]
		offsets[i + 1] = offsets[i] + (len(v))
		values = append(values, v...)
	}

	return ImmutableMemTable{
		Keys:    keys,
		Offsets: offsets,
		Values:  values,
	}
}

func (m ImmutableMemTable) Get(key int) []byte {
	bts := make([]byte, 4)
	binary.BigEndian.PutUint32(bts, uint32(key))
	h := farm.Fingerprint32(bts)
	return m.InterpolatedSearch(h)
}

func (m ImmutableMemTable) InterpolatedSearch(k uint32) []byte {
	if k < m.Keys[0] || k > m.Keys[len(m.Keys)-1] {
		return []byte{}
	}
	left := uint32(0)
	right := uint32(len(m.Keys) - 1)

	for left <= right {
		delta := float64(m.Keys[right] - m.Keys[left]) / float64(right - left)
		midFP := float64(k-m.Keys[left]) / delta
		mid := left + uint32(math.Min(float64(right-left-1), midFP))
		if m.Keys[mid] == k {
			return m.Values[m.Offsets[mid]:m.Offsets[mid + 1]]
		} else if m.Keys[mid] < k {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return nil
}
