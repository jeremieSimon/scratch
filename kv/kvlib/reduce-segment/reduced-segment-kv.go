package reduce_segment

import (
	"encoding/binary"
	farm "github.com/dgryski/go-farm"
	"hello/bytesort"
	"hello/kv/kvpb"
	"io/ioutil"
	"os"
)

type SegmentBasedKvBuilder struct {
	f             *os.File
	header        *kvpb.ReducedSegmentKvHeader
	nKeyInSegment int
	valueOffset   int64
	keyOffset     int64
}

type SegmentBasedKv struct {
	f      *os.File
	header *kvpb.ReducedSegmentKvHeader
}

func NewSegmentBasedKv(filename string) *SegmentBasedKv {
	f, _ := os.OpenFile(filename, os.O_RDWR, 0666)
	bigBuff := make([]byte, 16006)
	f.Read(bigBuff)

	var header kvpb.ReducedSegmentKvHeader
	header.Unmarshal(bigBuff)

	return &SegmentBasedKv{
		f:      f,
		header: &header,
	}
}

func NewSegmentBasedKvBuilder(filename string) *SegmentBasedKvBuilder {
	f, _ := os.Create(filename)

	// cheap hack to get a predictable size
	key := bytesort.NewSimpleKey(10, 10, 10)
	keySize := len(key.Marshal())
	nSegments := 1000

	segmentHeaders := make([]*kvpb.SegmentHeader, nSegments)
	for i := 0; i < nSegments; i++ {
		segmentOffset := keySize * 1000 * i
		segmentHeaders[i] = &kvpb.SegmentHeader{NKeys: 1, SegmentOffset: uint64(segmentOffset)}
	}
	header := kvpb.ReducedSegmentKvHeader{
		KeySize:        uint32(keySize),
		NSegments:      uint32(nSegments),
		SegmentSize:    uint32(nSegments) * uint32(keySize),
		SegmentHeaders: segmentHeaders,
	}

	headerBts, _ := header.Marshal()
	f.Write(headerBts)

	return &SegmentBasedKvBuilder{
		f:             f,
		header:        &header,
		nKeyInSegment: 1000,
		valueOffset:   750000000,
		keyOffset:     int64(header.Size()),
	}
}

func (b *SegmentBasedKvBuilder) Append(comparable uint32, value []byte) {
	// find segment number
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, comparable)
	hash := farm.Hash32WithSeed(bs, 9)
	segmentNumber := hash % b.header.NSegments

	// increment segment
	segmentHeader := b.header.SegmentHeaders[segmentNumber]
	segmentHeader.NKeys += 1

	// write value
	b.valueOffset -= int64(len(value))
	b.f.WriteAt(value, b.valueOffset)

	// build key
	key := bytesort.NewSimpleKey(comparable,  uint64(b.valueOffset), uint32(len(value)))

	// write key
	bs = key.Marshal()
	b.f.WriteAt(bs, b.keyOffset)
	b.keyOffset += int64(len(bs))
}

func (kv *SegmentBasedKv) Search(key uint32) []byte {
	// find the segment
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, key)
	hash := farm.Hash32WithSeed(bs, 9)
	segmentNumber := hash % kv.header.NSegments

	// search within that segment
	segment := kv.header.SegmentHeaders[segmentNumber]
	left := 0
	right := int(segment.NKeys) - 1

	keyBuffer := make([]byte, kv.header.KeySize)
	var comparableKey bytesort.IntBasedComparableKey

	// run binary search on the segment
	for left <= right {
		mid := (left + right) / 2
		midOffset := segment.SegmentOffset + uint64(uint32(mid)*kv.header.KeySize)
		kv.f.ReadAt(keyBuffer, int64(midOffset))

		comparableKey.Unmarshal(keyBuffer)

		if comparableKey.Comparable == key {
			valueBuffer := make([]byte, comparableKey.ValueLen)
			kv.f.ReadAt(valueBuffer, int64(comparableKey.ValueOffset))
			return valueBuffer
		} else if comparableKey.Comparable < key {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return []byte{}
}

func (b *SegmentBasedKvBuilder) Close() *SegmentBasedKv {
	// 1. adjust segment offset
	baseOffset := int64(b.header.Size())
	nKeysBefore := int64(0)

	segIdToOffset := make(map[uint32]int64)

	for i, segment := range b.header.SegmentHeaders {
		segment.SegmentOffset = uint64(baseOffset + (nKeysBefore * int64(b.header.KeySize)))
		segIdToOffset[uint32(i)] = int64(segment.SegmentOffset)
		nKeysBefore += int64(segment.NKeys) - 1
	}

	bigBuff := make([]byte, nKeysBefore * int64(b.header.KeySize))

	ioutil.WriteFile("tmp", bigBuff, 0666)
	tmp, _ := os.OpenFile("tmp", os.O_RDWR, 0666)
	// 2. read all keys,
	// write them to their proper segment in a temp file.
	keyBuffer := make([]byte, b.header.KeySize)
	keyOffset := int64(0)
	for i := 0; i < int(nKeysBefore); i++ {
		b.f.ReadAt(keyBuffer, baseOffset + keyOffset)
		var key bytesort.IntBasedComparableKey
		key.Unmarshal(keyBuffer)

		// find segment and write to correct segment
		bs := make([]byte, 4)
		binary.BigEndian.PutUint32(bs, key.Comparable)
		hash := farm.Hash32WithSeed(bs, 9)
		segmentId := hash % b.header.NSegments
		writeOffset := segIdToOffset[segmentId] - baseOffset
		tmp.WriteAt(keyBuffer, writeOffset)

		segIdToOffset[segmentId] += int64(b.header.KeySize)
		keyOffset += int64(b.header.KeySize)
	}

	// 3. write back segment per segment
	// read segment per segment in tmp file,
	// sort keys within each segment and write them back
	tmpOffset := int64(0)
	for segId, segment := range b.header.SegmentHeaders {
		// read key buffer
		startOffset := segment.SegmentOffset
		endOffset := segIdToOffset[uint32(segId)]
		keyBuffer := make([]byte, endOffset - int64(startOffset))
		tmp.ReadAt(keyBuffer, tmpOffset)

		// sort buffer
		comparableBuffer := bytesort.ComparableBuffer{
			KeyLen:      int(b.header.KeySize),
			NKeys:       int(segment.NKeys - 1),
			SmallerThan: bytesort.IntBasedComparableKey{}.SmallerThan,
		}

		sortedBuffer := comparableBuffer.Sort(keyBuffer)

		// write back the key buffer
		b.f.WriteAt(sortedBuffer, tmpOffset + baseOffset)
		tmpOffset += int64(len(keyBuffer))
	}
	headerBts, _ := b.header.Marshal()
	b.f.Write(headerBts)

	return &SegmentBasedKv{
		f:      b.f,
		header: b.header,
	}
}
