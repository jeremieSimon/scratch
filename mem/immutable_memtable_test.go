package mem

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/dgryski/go-farm"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

func TestMemTable_InterpolatedSearch(t *testing.T) {
	m := make(map[uint32][]byte)
	for i := 0; i < 5000000; i++ {
		bts := make([]byte, 4)
		n := rand.Intn(4)
		if n < 3 {
			binary.BigEndian.PutUint32(bts, uint32(i))
			h := farm.Fingerprint32(bts)
			m[h] = bts
		}
	}
	runtime.GC()
	time.Sleep(time.Second)
	PrintMemUsage()

	memTable := FromMap(m)
	runtime.GC()
	time.Sleep(time.Second)
	PrintMemUsage()

	memTable.Get(100)
}

func getRealSizeOf(v interface{}) (int, error) {
	b := new(bytes.Buffer)
	if err := gob.NewEncoder(b).Encode(v); err != nil {
		return 0, err
	}
	return b.Len(), nil
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
