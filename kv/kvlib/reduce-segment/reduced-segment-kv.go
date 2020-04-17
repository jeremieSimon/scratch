package reduce_segment

import (
	"encoding/binary"
	"fmt"
	"hello/kv/kvpb"
	"io/ioutil"
	"os"
	"sort"

	farm "github.com/dgryski/go-farm"
)

type SegmentBasedKvBuilder struct {
	f *os.File
	header *kvpb.ReducedSegmentKvHeader
	nKeyInSegment int
	valueOffset int64
}

type SegmentBasedKv struct {
	f *os.File
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
	sizeOfFile := 750000000
	bigBuff := make([]byte, sizeOfFile)
	ioutil.WriteFile(filename, bigBuff, 0666)

	f, _ := os.OpenFile(filename, os.O_RDWR, 0666)

	rsKey := kvpb.RSKey{
		Comparable:99,
		ValueLen:99,
		ValueOffset:99,
	}
	keySize := rsKey.Size()
	nSegments := 1000

	segmentHeaders := make([]*kvpb.SegmentHeader, nSegments)
	for i := 0; i < nSegments; i++ {
		segmentOffset := keySize * 1000 * i
		segmentHeaders[i] = &kvpb.SegmentHeader{NKeys:1, SegmentOffset:uint64(segmentOffset)}
	}
	header := kvpb.ReducedSegmentKvHeader{
		KeySize: uint32(keySize),
		NSegments:uint32(nSegments),
		SegmentSize: uint32(nSegments) * uint32(keySize),
		SegmentHeaders: segmentHeaders,
	}

	headerBts, _ := header.Marshal()
	f.Write(headerBts)

	return &SegmentBasedKvBuilder{
		f: f,
		header: &header,
		nKeyInSegment: 1000,
		valueOffset: 750000000,
	}
}

func (b SegmentBasedKvBuilder) Append(comparable uint32, value []byte) {
	// find segment number
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, comparable)
	hash := farm.Hash32WithSeed(bs, 9)
	segmentNumber := hash % b.header.NSegments

	// write value
	b.valueOffset -=  int64(len(value))
	b.f.WriteAt(value, b.valueOffset)

	// build key
	rsKey := kvpb.RSKey{
		Comparable:           comparable,
		ValueOffset:          uint64(b.valueOffset),
		ValueLen:             uint32(len(value)),
	}

	// write key
	segmentHeader := b.header.SegmentHeaders[segmentNumber]
	segmentOffset := segmentHeader.SegmentOffset + uint64(b.header.Size()) + uint64((segmentHeader.NKeys - 1) * b.header.KeySize)
	bs, _ = rsKey.Marshal()
	b.f.WriteAt(bs, int64(segmentOffset))

	segmentHeader.NKeys += 1
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
	var rsKey kvpb.RSKey

	// run binary search on the segment
	for left <= right {
		mid := (left + right) / 2
		midOffset := uint64(kv.header.Size()) + segment.SegmentOffset + uint64(uint32(mid) * kv.header.KeySize)
		kv.f.ReadAt(keyBuffer, int64(midOffset))

		rsKey.Unmarshal(keyBuffer)

		if rsKey.Comparable == key {
			valueBuffer := make([]byte, rsKey.ValueLen)
			kv.f.ReadAt(valueBuffer, int64(rsKey.ValueOffset))
			return valueBuffer
		} else if rsKey.Comparable < key {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return []byte{}

}

func (b SegmentBasedKvBuilder) Close() *SegmentBasedKv {
	// resort each segment
	for _, segmentHeader := range b.header.SegmentHeaders {

		keys := make([]*kvpb.RSKey, segmentHeader.NKeys - 1)

		startingOffset := int64(segmentHeader.SegmentOffset) + int64(b.header.Size())
		endingOffset := startingOffset + (int64(segmentHeader.NKeys - 1) * int64(b.header.KeySize))

		// resort key segment
		buffer := make([]byte, endingOffset - startingOffset)
		b.f.ReadAt(buffer, startingOffset)

		for i := 1; i < int(segmentHeader.NKeys); i++ {
			var leftEnd int
			if i == 0 {
				leftEnd = 0
			} else {
				leftEnd = (i - 1) * int(b.header.KeySize)
			}
			rightEnd := i * int(b.header.KeySize)
			var k kvpb.RSKey
			k.Unmarshal(buffer[leftEnd:rightEnd])
			keys[i - 1] = &k
		}

		sort.Slice(keys, func(i, j int) bool {
			return keys[i].Comparable < keys[j].Comparable
		})

		// write back the keys
		for nthKey, key := range keys {
			bts, _ := key.Marshal()
			b.f.WriteAt(bts, startingOffset + (int64(nthKey) * int64(b.header.KeySize)))
		}
	}

	headerBuffer, _ := b.header.Marshal()
	b.f.Write(headerBuffer)
	b.f.Close()

	fmt.Println(b.header.Size())

	return &SegmentBasedKv{
		f:      b.f,
		header: b.header,
	}
}



