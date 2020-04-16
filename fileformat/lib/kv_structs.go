package lib

import (
	"bytes"
	"encoding/binary"
)

// Simple file structure
type FileHeader struct {
	recordSize uint32
	nRecords uint32
}

func (f *FileHeader) Marshal() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, f.recordSize)
	binary.Write(buf, binary.BigEndian, f.nRecords)
	return buf.Bytes()
}

func (f *FileHeader) Unmarshal(bts []byte) {
	f.recordSize = binary.BigEndian.Uint32(bts[:4])
	f.nRecords = binary.BigEndian.Uint32(bts[4:])
}

type Person struct {
	age uint32
	sex bool
}

func (p Person) Marshal() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, p.age)
	binary.Write(buf, binary.BigEndian, p.sex)
	return buf.Bytes()
}

func (p *Person) Unmarshal(bts []byte) {
	p.age = binary.BigEndian.Uint32(bts[:4])
	if bts[4] == 1 {
		p.sex = true
	} else {
		p.sex = false
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
	comparable uint32
	offset uint64
	recordLength uint32
}

func (k Key) Marshal() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, k.comparable)
	binary.Write(buf, binary.BigEndian, k.offset)
	binary.Write(buf, binary.BigEndian, k.recordLength)
	return buf.Bytes()
}

func (k *Key) Unmarshal(bts []byte) {
	k.comparable = binary.BigEndian.Uint32(bts[:4])
	k.offset = binary.BigEndian.Uint64(bts[4:12])
	k.recordLength = binary.BigEndian.Uint32(bts[12:16])
}
