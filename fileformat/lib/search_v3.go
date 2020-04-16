package lib

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)

// TODO,
// 1. be agnostic towards the value type
// 2. be able to handle different types of key size
// 3. handle concurrent close + concurrent search
// 4. virtual table

// V3
type KeyValBuilder struct {
	f           *os.File
	nKeys       int
	keys        []*Key
	segmentSize int
	keyOffset   int64
	valueOffset int64
	header      *FileHeader
	headerLen   int64
}

type KeyValueShard struct {
	header      *FileHeader
	f           *os.File
}

func NewKeyValueShard(filename string) KeyValueShard{
	f, _ := os.Open(filename)
	headerOffset := make([]byte, 8)
	f.Read(headerOffset)

	var header FileHeader
	header.Unmarshal(headerOffset)

	return KeyValueShard{&header, f}
}

func (kvShard *KeyValueShard) Search(key uint32) []byte {

	left := int64(0)
	right := int64(kvShard.header.nRecords)

	keyBuffer := make([]byte, 16)
	originalOffset := 8

	for left <= right {
		mid := (left + right) / 2
		s := int64(originalOffset) + (mid * int64(kvShard.header.recordSize))
		kvShard.f.ReadAt(keyBuffer, s)

		var k Key
		k.Unmarshal(keyBuffer)
		if k.comparable == key {
			valueBuff := make([]byte, k.recordLength)
			kvShard.f.ReadAt(valueBuff, int64(k.offset))
			return valueBuff

		} else if k.comparable < key {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return nil
}

func NewKeyValBuilder(filename string) *KeyValBuilder {
	sizeOfFile := 750000000
	bigBuff := make([]byte, sizeOfFile)
	ioutil.WriteFile(filename, bigBuff, 0666)
	f, _ := os.OpenFile(filename, os.O_RDWR, 0666)

	header := FileHeader{16, 50000000}
	f.Write(header.Marshal())

	return &KeyValBuilder{f, 0, make([]*Key, 0), 1000, 8, 750000000, &header, 8}
}

func (k *KeyValBuilder) Append(key uint32, value []byte) error {
	k.valueOffset -= int64(len(value))
	k.f.WriteAt(value, k.valueOffset)

	kkey := &Key{key, uint64(k.valueOffset),uint32(len(value))}
	k.keys = append(k.keys, kkey)
	k.nKeys += 1

	if k.nKeys == int(k.header.nRecords) {
		return errors.New("max size reached")
	}
	if len(k.keys) == k.segmentSize {
		k.writeKeys()
	}
	return nil
}

func (k *KeyValBuilder) Close() KeyValueShard {
	k.writeKeys()
	k.mergeSegments()
	k.header.nRecords = uint32(k.nKeys)
	k.f.WriteAt(k.header.Marshal(), 0)
	return KeyValueShard {
		k.header, k.f,
	}
}

func (k *KeyValBuilder) writeKeys() {
	// sort the keys in  memory
	sort.Slice(k.keys, func(i, j int) bool {
		return k.keys[i].comparable <= k.keys[j].comparable
	})

	// write keys
	for _, key := range k.keys {
		bts := key.Marshal()
		k.f.WriteAt(bts, k.keyOffset)
		k.keyOffset += int64(len(bts))
	}
	k.keys = make([]*Key, 0)
}

func (k *KeyValBuilder) mergeSegments() {
	type SegmentRange struct {
		left int64
		right int64
	}
	segmentsToMerge := make([]*SegmentRange, 0)
	maxRight := (int64(k.nKeys) * int64(k.header.recordSize)) + 8
	for i := 0; i < k.nKeys; i += k.segmentSize {
		left := (int64(i) * int64(k.header.recordSize)) + 8
		right := (int64(i + k.segmentSize) * int64(k.header.recordSize)) + 8
		if right > maxRight {
			right = maxRight
		}
		segmentsToMerge = append(segmentsToMerge, &SegmentRange{left, right})
	}

	nextSegmentsToMerge := make([]*SegmentRange, 0)
	for len(segmentsToMerge) > 0 {

		if len(segmentsToMerge) >= 2 {
			leftSegment := segmentsToMerge[0]
			rightSegment := segmentsToMerge[1]
			k.merge(leftSegment.left, leftSegment.right, rightSegment.left, rightSegment.right)
			segmentsToMerge = segmentsToMerge[2:]
			nextSegmentsToMerge = append(nextSegmentsToMerge, &SegmentRange{leftSegment.left, rightSegment.right})
		}

		// if len is 1
		if len(segmentsToMerge) == 1 {
			nextSegmentsToMerge = append(nextSegmentsToMerge, segmentsToMerge[0])
			segmentsToMerge = []*SegmentRange{}
		}

		// drain next to current
		if len(segmentsToMerge) == 0 && len(nextSegmentsToMerge) > 1 {
			for _, seg := range nextSegmentsToMerge {
				segmentsToMerge = append(segmentsToMerge, seg)
			}
			nextSegmentsToMerge = []*SegmentRange{}
		}
	}
}

func (k *KeyValBuilder) merge(leftSegmentStart, leftSegmentEnd, rightSegmentStart, rightSegmentEnd int64) {
	os.Remove("tmp")
	tmpBuff, _ := os.Create("tmp")

	leftOffset := leftSegmentStart
	rightOffset := rightSegmentStart

	var leftK Key
	var rightK Key
	leftKeyBuffer := make([]byte, k.header.recordSize)
	rightKeyBuffer := make([]byte, k.header.recordSize)

	for leftOffset < leftSegmentEnd && rightOffset < rightSegmentEnd {
		k.f.ReadAt(leftKeyBuffer, leftOffset)
		leftK.Unmarshal(leftKeyBuffer)

		k.f.ReadAt(rightKeyBuffer, rightOffset)
		rightK.Unmarshal(rightKeyBuffer)

		if leftK.comparable < rightK.comparable {
			tmpBuff.Write(leftKeyBuffer)
			leftOffset += 16
		} else {
			tmpBuff.Write(rightKeyBuffer)
			rightOffset += 16
		}
	}

	for leftOffset < leftSegmentEnd {
		k.f.ReadAt(leftKeyBuffer, leftOffset)
		tmpBuff.Write(leftKeyBuffer)
		leftOffset += 16
	}

	for rightOffset < rightSegmentEnd {
		k.f.ReadAt(rightKeyBuffer, rightOffset)
		tmpBuff.Write(rightKeyBuffer)
		rightOffset += 16
	}

	tmpBuff.Close()
	tmpBuff, _ = os.Open("tmp")

	// write back to original segments
	stepSize := 100
	bigBuffer := make([]byte, 16 * stepSize)
	nKeys := (rightSegmentEnd - leftSegmentStart) / 16

	for i := 0; i < int(nKeys); i += stepSize {
		tmpBuff.ReadAt(bigBuffer, int64(i * 16))

		off := leftSegmentStart + int64(i * 16)
		k.f.WriteAt(bigBuffer, off)
	}
}

func (k *KeyValBuilder) readKeys() {
	keys := make([]*Key, k.nKeys)
	keyBuffer := make([]byte, 16)

	for i := 0; i < k.nKeys; i ++ {
		k.f.Seek(int64((i * 16) + 8), 0)
		k.f.Read(keyBuffer)
		var key Key
		key.Unmarshal(keyBuffer)
		keys[i] = &key
		fmt.Println(key)
	}
}