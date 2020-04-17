package kvpb

import (
	"bytes"
	"encoding/binary"
)

// Simple file structure
type FileHeader struct {
	RecordSize uint32
	NRecords   uint32
}

func (f *FileHeader) Marshal() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, f.RecordSize)
	binary.Write(buf, binary.BigEndian, f.NRecords)
	return buf.Bytes()
}

func (f *FileHeader) Unmarshal(bts []byte) {
	f.RecordSize = binary.BigEndian.Uint32(bts[:4])
	f.NRecords = binary.BigEndian.Uint32(bts[4:])
}

type Person struct {
	Age uint32
	Sex bool
}

func (p Person) Marshal() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, p.Age)
	binary.Write(buf, binary.BigEndian, p.Sex)
	return buf.Bytes()
}

func (p *Person) Unmarshal(bts []byte) {
	p.Age = binary.BigEndian.Uint32(bts[:4])
	if bts[4] == 1 {
		p.Sex = true
	} else {
		p.Sex = false
	}
}

// impl 2
type ComplexPerson struct {
	Age  uint32
	Name string
}

func (p ComplexPerson) Marshal() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, p.Age)
	for _, r := range p.Name {
		binary.Write(buf, binary.BigEndian, int8(r))
	}
	return buf.Bytes()
}

func (p *ComplexPerson) Unmarshal(bts []byte) {
	p.Age = binary.BigEndian.Uint32(bts[:4])
	p.Name = string(bts[4:])
}

type SimpleKeyValue struct {
	key Key
	value []byte
}

type Key struct {
	Comparable   uint32
	Offset       uint64
	RecordLength uint32
}

func (k Key) Marshal() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, k.Comparable)
	binary.Write(buf, binary.BigEndian, k.Offset)
	binary.Write(buf, binary.BigEndian, k.RecordLength)
	return buf.Bytes()
}

func (k *Key) Unmarshal(bts []byte) {
	k.Comparable = binary.BigEndian.Uint32(bts[:4])
	k.Offset = binary.BigEndian.Uint64(bts[4:12])
	k.RecordLength = binary.BigEndian.Uint32(bts[12:16])
}
