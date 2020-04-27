package simple

import (
	"errors"
	"fmt"
	"hello/kv/kvpb"
	"os"
	"sort"
)

// TODO,
// 1. be able to handle different types of key size
// 2. handle concurrent close + concurrent search

// V3
type KeyValBuilder struct {
	f           *os.File
	nKeys       int
	keys        []*kvpb.Key
	segmentSize int
	keyOffset   int64
	valueOffset int64
	header      *kvpb.FileHeader
	headerLen   int64
}

type KeyValueShard struct {
	header *kvpb.FileHeader
	f      *os.File
	mem    *BSTNode
}

func NewKeyValueShard(filename string) KeyValueShard {
	f, _ := os.Open(filename)
	headerOffset := make([]byte, 8)
	f.Read(headerOffset)

	var header kvpb.FileHeader
	header.Unmarshal(headerOffset)

	mem := NewInMemIndex(f, &header)
	return KeyValueShard{&header, f, mem}
}

type BSTNode struct {
	key       kvpb.Key
	keyOffset int64
	left      *BSTNode
	right     *BSTNode
}

func NewNode(k kvpb.Key, keyOffset int64) *BSTNode {
	return &BSTNode{key: k, keyOffset: keyOffset}
}

func NewInMemIndex(f *os.File, header *kvpb.FileHeader) *BSTNode {
	left := int64(0)
	right := int64(header.NRecords)

	keyBuffer := make([]byte, 16)
	mid := (left + right) / 2
	s := 8 + (mid * int64(header.RecordSize))
	f.ReadAt(keyBuffer, s)
	var k kvpb.Key
	k.Unmarshal(keyBuffer)

	root := NewNode(k, s)
	type NodeAndRange struct {
		node  *BSTNode
		left  int64
		right int64
	}
	counter := 1000
	q := []*NodeAndRange{{root, left, right}}

	for counter > 0 {
		nodeAndRange := q[0]
		node := nodeAndRange.node
		q = q[1:]
		mid := (nodeAndRange.left + nodeAndRange.right) / 2

		// build left node
		midLeft := (nodeAndRange.left + mid) / 2
		s = 8 + (midLeft * int64(header.RecordSize))
		f.ReadAt(keyBuffer, s)
		k.Unmarshal(keyBuffer)
		leftNode := NewNode(k, s)
		node.left = leftNode

		// build right node
		midRight := (mid + nodeAndRange.right) / 2
		s = 8 + (midRight * int64(header.RecordSize))
		f.ReadAt(keyBuffer, s)
		k.Unmarshal(keyBuffer)
		rightNode := NewNode(k, s)
		node.right = rightNode

		q = append(q, &NodeAndRange{leftNode, nodeAndRange.left, mid})
		q = append(q, &NodeAndRange{rightNode, mid, nodeAndRange.right})

		counter -= 2
	}
	return root
}

func (kvShard *KeyValueShard) Search(key uint32) []byte {
	left := int64(0)
	right := (int64(kvShard.header.NRecords) * 16) + 8
	node := kvShard.mem
	for node != nil {
		if key == node.key.Comparable {
			valueBuff := make([]byte, node.key.RecordLength)
			kvShard.f.ReadAt(valueBuff, int64(node.key.Offset))
			return valueBuff
		} else if key < node.key.Comparable {
			right = node.keyOffset
			node = node.left
		} else {
			left = node.keyOffset
			node = node.right
		}
	}
	return kvShard.OnDiskSearch(key, left, right)
}

func (kvShard *KeyValueShard) OnDiskSearch(key uint32, leftOffset, rightOffset int64) []byte {

	left := (leftOffset - 8) / 16
	right := (rightOffset - 8) / 16

	keyBuffer := make([]byte, 16)
	originalOffset := 8

	for left <= right {
		mid := (left + right) / 2
		s := int64(originalOffset) + (mid * int64(kvShard.header.RecordSize))
		kvShard.f.ReadAt(keyBuffer, s)

		var k kvpb.Key
		k.Unmarshal(keyBuffer)
		if k.Comparable == key {
			valueBuff := make([]byte, k.RecordLength)
			kvShard.f.ReadAt(valueBuff, int64(k.Offset))
			return valueBuff

		} else if k.Comparable < key {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return nil
}

func NewKeyValBuilder(filename string) *KeyValBuilder {
	f, _ := os.Create(filename)

	header := kvpb.FileHeader{16, 50000000}
	f.Write(header.Marshal())

	return &KeyValBuilder{f, 0, make([]*kvpb.Key, 0), 1000, 8, 750000000, &header, 8}
}

func (k *KeyValBuilder) Append(key uint32, value []byte) error {
	k.valueOffset -= int64(len(value))
	k.f.WriteAt(value, k.valueOffset)

	kkey := &kvpb.Key{key, uint64(k.valueOffset), uint32(len(value))}
	k.keys = append(k.keys, kkey)
	k.nKeys += 1

	if k.nKeys == int(k.header.NRecords) {
		return errors.New("max size reached")
	}
	if len(k.keys) == k.segmentSize {
		k.writeKeys()
	}
	return nil
}

func (k *KeyValBuilder) Close() KeyValueShard {
	// flush keys
	k.writeKeys()

	// merge segments
	k.mergeSegments()

	k.header.NRecords = uint32(k.nKeys)
	k.f.WriteAt(k.header.Marshal(), 0)

	inMem := NewInMemIndex(k.f, k.header)

	return KeyValueShard{
		k.header, k.f, inMem,
	}
}

func (k *KeyValBuilder) writeKeys() {
	// sort the keys in  memory
	sort.Slice(k.keys, func(i, j int) bool {
		return k.keys[i].Comparable <= k.keys[j].Comparable
	})

	// write keys
	for _, key := range k.keys {
		bts := key.Marshal()
		k.f.WriteAt(bts, k.keyOffset)
		k.keyOffset += int64(len(bts))
	}
	k.keys = make([]*kvpb.Key, 0)
}

func (k *KeyValBuilder) mergeSegments() {
	type SegmentRange struct {
		left  int64
		right int64
	}
	segmentsToMerge := make([]*SegmentRange, 0)
	maxRight := (int64(k.nKeys) * int64(k.header.RecordSize)) + 8
	for i := 0; i < k.nKeys; i += k.segmentSize {
		left := (int64(i) * int64(k.header.RecordSize)) + 8
		right := (int64(i+k.segmentSize) * int64(k.header.RecordSize)) + 8
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

	var leftK kvpb.Key
	var rightK kvpb.Key
	leftKeyBuffer := make([]byte, k.header.RecordSize)
	rightKeyBuffer := make([]byte, k.header.RecordSize)

	for leftOffset < leftSegmentEnd && rightOffset < rightSegmentEnd {
		k.f.ReadAt(leftKeyBuffer, leftOffset)
		leftK.Unmarshal(leftKeyBuffer)

		k.f.ReadAt(rightKeyBuffer, rightOffset)
		rightK.Unmarshal(rightKeyBuffer)

		if leftK.Comparable < rightK.Comparable {
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
	bigBuffer := make([]byte, 16*stepSize)
	nKeys := (rightSegmentEnd - leftSegmentStart) / 16

	for i := 0; i < int(nKeys); i += stepSize {
		tmpBuff.ReadAt(bigBuffer, int64(i*16))

		off := leftSegmentStart + int64(i*16)
		k.f.WriteAt(bigBuffer, off)
	}
}

func (k *KeyValBuilder) readKeys() {
	keys := make([]*kvpb.Key, k.nKeys)
	keyBuffer := make([]byte, 16)

	for i := 0; i < k.nKeys; i++ {
		k.f.Seek(int64((i*16)+8), 0)
		k.f.Read(keyBuffer)
		var key kvpb.Key
		key.Unmarshal(keyBuffer)
		keys[i] = &key
		fmt.Println(key)
	}
}
