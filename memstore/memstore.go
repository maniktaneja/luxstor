package memstore

import (
	"bytes"
	"errors"
	"math/rand"
	"sync/atomic"
)

const DiskBlockSize = 512 * 1024

var (
	ErrNotEnoughSpace = errors.New("Not enough space in the buffer")
)

type KeyCompare func([]byte, []byte) int

type Item struct {
	bornSn, deadSn uint32
	data           []byte
}

func (itm *Item) Bytes() []byte {
	return itm.data
}

func NewItem(data []byte) *Item {
	return &Item{
		data: data,
	}
}

func newInsertCompare(keyCmp KeyCompare) CompareFn {
	return func(this SLItem, that SLItem) int {
		var v int
		thisItem := this.(*Item)
		thatItem := that.(*Item)
		if v = keyCmp(thisItem.data, thatItem.data); v == 0 {
			v = int(thisItem.bornSn) - int(thatItem.bornSn)
		}

		return v
	}
}

func newIterCompare(keyCmp KeyCompare) CompareFn {
	return func(this SLItem, that SLItem) int {
		thisItem := this.(*Item)
		thatItem := that.(*Item)
		return keyCmp(thisItem.data, thatItem.data)
	}
}

func defaultKeyCmp(this []byte, that []byte) int {
	var l int

	l1 := len(this)
	l2 := len(that)
	if l1 < l2 {
		l = l1
	} else {
		l = l2
	}

	return bytes.Compare(this[:l], that[:l])
}

//
//compare item,sn
type Writer struct {
	rand *rand.Rand
	buf  *ActionBuffer
	iter *SLIterator
	*MemStore
}

func (w *Writer) Put(x *Item) {
	x.bornSn = w.getCurrSn()
	w.store.Insert2(x, w.insCmp, w.buf, w.rand.Float32)
	atomic.AddInt64(&w.count, 1)
}

// Find first item, seek until dead=0, mark dead=sn
func (w *Writer) Delete(x *Item) (success bool) {
	defer func() {
		if success {
			atomic.AddInt64(&w.count, -1)
		}
	}()

	gotItem := w.Get(x)
	if gotItem != nil {
		sn := w.getCurrSn()
		if gotItem.bornSn == sn {
			success = w.store.Delete(gotItem, w.insCmp, w.buf)
			return
		}

		success = atomic.CompareAndSwapUint32(&gotItem.deadSn, 0, sn)
		return
	}

	return
}

func (w *Writer) Get(x *Item) *Item {
	var curr *Item
	found := w.iter.Seek(x)
	if !found {
		return nil
	}

	// Seek until most recent item for key is found
	curr = w.iter.Get().(*Item)
	for {
		w.iter.Next()
		if !w.iter.Valid() {
			break
		}
		next := w.iter.Get().(*Item)
		if w.iterCmp(next, curr) != 0 {
			break
		}

		curr = next
	}

	if curr.deadSn != 0 {
		return nil
	}

	return curr
}

type MemStore struct {
	store       *Skiplist
	currSn      uint32
	snapshots   *Skiplist
	isGCRunning int32
	lastGCSn    uint32
	count       int64

	keyCmp  KeyCompare
	insCmp  CompareFn
	iterCmp CompareFn
}

func New() *MemStore {
	m := &MemStore{
		store:     NewSkiplist(),
		snapshots: NewSkiplist(),
		currSn:    1,
	}

	m.SetKeyComparator(defaultKeyCmp)
	return m
}

func (m *MemStore) SetKeyComparator(cmp KeyCompare) {
	m.keyCmp = cmp
	m.insCmp = newInsertCompare(cmp)
	m.iterCmp = newIterCompare(cmp)
}

func (m *MemStore) getCurrSn() uint32 {
	return atomic.LoadUint32(&m.currSn)
}

func (m *MemStore) NewWriter() *Writer {
	buf := m.store.MakeBuf()

	return &Writer{
		rand:     rand.New(rand.NewSource(int64(rand.Int()))),
		buf:      buf,
		iter:     m.store.NewSLIterator(m.iterCmp, buf),
		MemStore: m,
	}
}

type Snapshot struct {
	sn       uint32
	refCount int32
	db       *MemStore
	count    int64
}

func (s Snapshot) Count() int64 {
	return s.count
}

func (s *Snapshot) Open() bool {
	if atomic.LoadInt32(&s.refCount) == 0 {
		return false
	}
	atomic.AddInt32(&s.refCount, 1)
	return true
}

func (s *Snapshot) Close() {
	newRefcount := atomic.AddInt32(&s.refCount, -1)
	if newRefcount == 0 {
		buf := s.db.snapshots.MakeBuf()
		s.db.snapshots.Delete(s, CompareSnapshot, buf)
		if atomic.CompareAndSwapInt32(&s.db.isGCRunning, 0, 1) {
			go s.db.GC()
		}
	}
}

func (s *Snapshot) NewIterator() *Iterator {
	return s.db.NewIterator(s)
}

func CompareSnapshot(this SLItem, that SLItem) int {
	thisItem := this.(*Snapshot)
	thatItem := that.(*Snapshot)

	return int(thisItem.sn) - int(thatItem.sn)
}

func (m *MemStore) NewSnapshot() *Snapshot {
	buf := m.snapshots.MakeBuf()

	snap := &Snapshot{db: m, sn: m.getCurrSn(), refCount: 1, count: m.ItemsCount()}
	m.snapshots.Insert(snap, CompareSnapshot, buf)
	atomic.AddUint32(&m.currSn, 1)
	return snap
}

type Iterator struct {
	snap *Snapshot
	iter *SLIterator
	buf  *ActionBuffer
}

func (it *Iterator) skipUnwanted() {
loop:
	if !it.iter.Valid() {
		return
	}
	itm := it.iter.Get().(*Item)
	if itm.bornSn > it.snap.sn || (itm.deadSn > 0 && itm.deadSn <= it.snap.sn) {
		it.iter.Next()
		goto loop
	}
}

func (it *Iterator) SeekFirst() {
	it.iter.SeekFirst()
	it.skipUnwanted()
}

func (it *Iterator) Seek(itm *Item) {
	it.iter.Seek(itm)
	it.skipUnwanted()
}

func (it *Iterator) Valid() bool {
	return it.iter.Valid()
}

func (it *Iterator) Get() *Item {
	return it.iter.Get().(*Item)
}

func (it *Iterator) Next() {
	it.iter.Next()
	it.skipUnwanted()
}

func (it *Iterator) Close() {
	it.snap.Close()
}

func (m *MemStore) NewIterator(snap *Snapshot) *Iterator {
	if snap == nil {
		snap = &Snapshot{sn: m.getCurrSn()}
	} else if !snap.Open() {
		return nil
	}

	buf := m.store.MakeBuf()
	return &Iterator{
		snap: snap,
		iter: m.store.NewSLIterator(m.iterCmp, buf),
		buf:  buf,
	}
}

func (m *MemStore) ItemsCount() int64 {
	return atomic.LoadInt64(&m.count)
}

func (m *MemStore) Rollback(snap *Snapshot) {
	buf1 := m.snapshots.MakeBuf()
	buf2 := m.snapshots.MakeBuf()
	iter := m.store.NewSLIterator(m.iterCmp, buf1)
	iter.SeekFirst()
	for ; iter.Valid(); iter.Next() {
		itm := iter.Get().(*Item)
		if itm.bornSn > snap.sn || itm.deadSn > snap.sn {
			m.store.Delete(itm, m.insCmp, buf2)
		}
	}

	m.currSn = snap.sn
	m.lastGCSn = snap.sn - 1
}

func (m *MemStore) collectDead(sn uint32) {
	buf1 := m.snapshots.MakeBuf()
	buf2 := m.snapshots.MakeBuf()
	iter := m.store.NewSLIterator(m.iterCmp, buf1)
	iter.SeekFirst()
	for ; iter.Valid(); iter.Next() {
		itm := iter.Get().(*Item)
		if itm.deadSn > 0 && itm.deadSn <= sn {
			m.store.Delete(itm, m.insCmp, buf2)
		}
	}
}

func (m *MemStore) GC() {
	buf := m.snapshots.MakeBuf()

	iter := m.snapshots.NewSLIterator(CompareSnapshot, buf)
	iter.SeekFirst()
	if iter.Valid() {
		snap := iter.Get().(*Snapshot)
		if snap.sn != m.lastGCSn && snap.sn > 1 {
			m.lastGCSn = snap.sn - 1
			m.collectDead(m.lastGCSn)
		}
	}

	atomic.CompareAndSwapInt32(&m.isGCRunning, 1, 0)
}

func (m *MemStore) GetSnapshots() []*Snapshot {
	var snaps []*Snapshot
	buf := m.snapshots.MakeBuf()
	iter := m.snapshots.NewSLIterator(CompareSnapshot, buf)
	iter.SeekFirst()
	for ; iter.Valid(); iter.Next() {
		snaps = append(snaps, iter.Get().(*Snapshot))
	}

	return snaps
}

func (m *MemStore) DumpStats() string {
	return m.store.GetStats().String()
}
