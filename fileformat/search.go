package main

import (
	"fmt"
	"hello/fileformat/lib"
)


func main() {
	//writeSimpleFile("test")
	//toKeyValFile("bigfile")
	//fmt.Println("dd")
	//fmt.Println(onFileBinarySearch("test", 11))
	//binarySearchToKeyValFile("bigfile", 1)


	//kvBuilder := lib.NewKeyValBuilder("bigfile_v3")
	//for i := 0; i < 1000000; i++ {
	//	c := lib.ComplexPerson{Age:uint32(i), Name:fmt.Sprintf("name is %d", i)}
	//	kvBuilder.Append(c.Age, c.Marshal())
	//}
	//kvShard := kvBuilder.Close()

	kvShard := lib.NewKeyValueShard("bigfile_v3")
	var complexPerson lib.ComplexPerson
	var bts []byte

	bts = kvShard.Search(1049)
	complexPerson.Unmarshal(bts)
	fmt.Println(complexPerson)

	bts = kvShard.Search(14)
	complexPerson.Unmarshal(bts)
	fmt.Println(complexPerson)

	bts = kvShard.Search(2002)
	complexPerson.Unmarshal(bts)
	fmt.Println(complexPerson)

	bts = kvShard.Search(8064)
	complexPerson.Unmarshal(bts)
	fmt.Println(complexPerson)
}

//func readTmp(name string, n int) {
//	tmpBuff, _ := os.Open(name)
//	bigBuffer := make([]byte, 16)
//	var k lib.Key
//	fmt.Println("\n\n--------")
//	for i := 0; i < n; i++ {
//		tmpBuff.ReadAt(bigBuffer, int64(i * 16))
//		k.Unmarshal(bigBuffer)
//		fmt.Println(k)
//	}
//}
//
//// V2
//func binarySearchToKeyValFile(filename string, age uint32) {
//	file, _ := os.Open(filename)
//
//	headerBuffer := make([]byte, 8)
//	file.Read(headerBuffer)
//	var header lib.FileHeader
//	header.Unmarshal(headerBuffer)
//
//	left := int64(0)
//	right := int64(10)
//
//	keyBuffer := make([]byte, 16)
//	originalOffset := 8
//
//	for left <= right {
//		mid := (left + right) / 2
//		s := int64(originalOffset) + (mid * int64(header.recordSize))
//		file.Seek(s, 0)
//		file.Read(keyBuffer)
//
//		var k lib.Key
//		k.Unmarshal(keyBuffer)
//		if k.comparable == age {
//			valueBuff := make([]byte, k.recordLength)
//			file.Seek(int64(k.offset), 0)
//			file.Read(valueBuff)
//
//			var complexPerson lib.ComplexPerson
//			complexPerson.Unmarshal(valueBuff)
//			fmt.Println(complexPerson)
//			return
//
//		} else if k.comparable < age {
//			left = mid + 1
//		} else {
//			right = mid - 1
//		}
//	}
//	fmt.Println("nothing")
//
//
//}
//
//func toKeyValFile(filename string) {
//	// create an empty file
//	sizeOfFile := 7500000000
//	bigBuff := make([]byte, sizeOfFile)
//	ioutil.WriteFile(filename, bigBuff, 0666)
//
//	// write down some values
//	file, _ := os.OpenFile(filename, os.O_RDWR, 0666)
//	header := lib.FileHeader{16, 50000000}
//	file.Write(header.Marshal())
//
//	writingOffset := int64(sizeOfFile)
//	writingKeyOffset := int64(8)
//
//	for i := 1; i < 50000000; i++ {
//		if i % 10000 == 0 {
//			fmt.Println(i)
//		}
//		complexPerson := lib.ComplexPerson{uint32(i), fmt.Sprintf("this is the value %d ", i)}
//		bts := complexPerson.Marshal()
//
//		writingOffset -= int64(len(bts))
//		kv := lib.SimpleKeyValue{key: lib.Key{uint32(i), uint64(writingOffset), uint32(len(bts))}, value: bts}
//
//		// write key
//		file.Seek(writingKeyOffset, 0)
//		file.Write(kv.key.Marshal())
//		writingKeyOffset += 16
//
//		// write value
//		file.Seek(writingOffset, 0)
//		file.Write(kv.value)
//	}
//
//	file.Close()
//}
//
//// V1
//func writeSimpleFile(filename string) {
//	header := lib.FileHeader{5, 100}
//
//	bts := make([]byte, 0)
//	bts = append(bts, header.Marshal()...)
//
//	for i := 0; i < 100; i++ {
//		bts = append(bts, lib.Person{uint32(i), true}.Marshal()...)
//	}
//	ioutil.WriteFile(filename, bts, 0777)
//}
//func onFileBinarySearch(fileName string, targetAge uint32) lib.Person {
//
//	file, _ := os.Open(fileName)
//
//	// get the file header
//	buff := make([]byte, 10)
//	file.Read(buff)
//	var h lib.FileHeader
//	h.Unmarshal(buff)
//
//	left := int64(0)
//	right := int64(h.nRecords)
//	var p lib.Person
//
//	for left <= right {
//
//		mid := (left + right) / 2
//		file.Seek(mid * int64(h.recordSize) + 8, 0)
//		file.Read(buff)
//		p.Unmarshal(buff)
//
//		if p.age == targetAge {
//			return p
//		} else if p.age < targetAge {
//			left = mid + 1
//		} else {
//			right = mid - 1
//		}
//	}
//	return p
//}