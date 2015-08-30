package memstore

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

func doInsert(db *MemStore, wg *sync.WaitGroup, n int, isRand bool, shouldSnap bool) {
	defer wg.Done()
	w := db.NewWriter()
	rnd := rand.New(rand.NewSource(int64(rand.Int())))
	for i := 0; i < n; i++ {
		var val int
		if isRand {
			val = rnd.Int()
		} else {
			val = i
		}
		if shouldSnap && i%100000 == 0 {
			s := w.NewSnapshot()
			s.Close()
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(val))
		// w.Put(NewItem([]byte(fmt.Sprintf("%025d", val))))
		w.Put(NewItem(buf))
	}
}

func TestInsertPerf(t *testing.T) {
	var wg sync.WaitGroup
	db := New()
	n := 1000000
	t0 := time.Now()
	total := n * runtime.GOMAXPROCS(0)
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go doInsert(db, &wg, n, true, true)
	}
	wg.Wait()

	dur := time.Since(t0)
	fmt.Printf("%d items took %v -> %v items/s snapshots_created %v live_snapshots %v\n",
		total, dur, float64(total)/float64(dur.Seconds()), db.getCurrSn(), len(db.GetSnapshots()))
}
