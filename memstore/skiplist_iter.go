package memstore

type SLIterator struct {
	cmp        CompareFn
	s          *Skiplist
	prev, curr *Node
	valid      bool
	buf        *ActionBuffer
}

func (s *Skiplist) NewSLIterator(cmp CompareFn,
	buf *ActionBuffer) *SLIterator {

	return &SLIterator{
		cmp: cmp,
		s:   s,
		buf: buf,
	}
}

func (it *SLIterator) SeekFirst() {
	it.prev = it.s.head
	it.curr, _ = it.s.head.getNext(0)
	it.valid = true
}

func (it *SLIterator) Seek(itm SLItem) bool {
	it.valid = true
	found := it.s.findPath(itm, it.cmp, it.buf)
	it.prev = it.buf.preds[0]
	it.curr = it.buf.succs[0]
	return found
}

func (it *SLIterator) Valid() bool {
	if it.valid && it.curr == it.s.tail {
		it.valid = false
	}

	return it.valid
}

func (it *SLIterator) Get() SLItem {
	return it.curr.itm
}

func (it *SLIterator) Next() {
retry:
	it.valid = true
	next, deleted := it.curr.getNext(0)
	for deleted {
		if !it.s.helpDelete(0, it.prev, it.curr, next) {
			found := it.s.findPath(it.curr.itm, it.cmp, it.buf)
			last := it.curr
			it.prev = it.buf.preds[0]
			it.curr = it.buf.succs[0]
			if found && last == it.curr {
				goto retry
			} else {
				return
			}
		}
		it.curr, _ = it.prev.getNext(0)
		next, deleted = it.curr.getNext(0)
	}

	it.prev = it.curr
	it.curr = next
}
