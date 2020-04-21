package bytesort

import (
	"sort"
)

// Sort buffer without deser values inside it.
type ComparableBuffer struct {
	KeyLen      int
	NKeys       int
	SmallerThan func([]byte, []byte) bool
}

func (b ComparableBuffer) Sort(orignalBuff []byte) []byte {

	keys := make([]int, b.NKeys)
	for i := 0; i < b.NKeys; i++ {
		keys[i] = i
	}

	genBounds := func(i int) (int, int) {
		left := i * b.KeyLen
		right := left + b.KeyLen
		return left, right
	}

	sort.Slice(keys, func(i, j int) bool {
		iLeft, iRight := genBounds(keys[i])
		jLeft, jRight := genBounds(keys[j])
		return b.SmallerThan(orignalBuff[iLeft:iRight], orignalBuff[jLeft:jRight])
	})

	sortedBuff := make([]byte, len(orignalBuff))
	writingIndex := 0

	for _, k := range keys {
		left, right := genBounds(k)
		for i := left; i < right; i++ {
			sortedBuff[writingIndex] = orignalBuff[i]
			writingIndex += 1
		}
	}

	return sortedBuff
}
