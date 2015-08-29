package main

import (
	"encoding/binary"
	"github.com/couchbase/gomemcached"
	"github.com/maniktaneja/luxstor/memstore"
	"github.com/scryner/lfreequeue"
	"log"
	"runtime"
)

type storage struct {
	data map[string]gomemcached.MCItem
	cas  uint64
}

type handler func(req *gomemcached.MCRequest, s *luxStor) *gomemcached.MCResponse

var handlers = map[gomemcached.CommandCode]handler{
	gomemcached.SET:    handleSet,
	gomemcached.GET:    handleGet,
	gomemcached.DELETE: handleDelete,
	gomemcached.FLUSH:  handleFlush,
}

type luxStor struct {
	memdb     *memstore.MemStore
	workQueue *lfreequeue.Queue
}

// init memdb
func initMemdb() *luxStor {

	runtime.GOMAXPROCS(runtime.NumCPU())

	ls := &luxStor{memdb: memstore.New()}
	ls.memdb.SetKeyComparator(byteItemKeyCompare)
	ls.workQueue = lfreequeue.NewQueue()

	// create a queue of writers
	for i := 0; i < 128; i++ {
		w := ls.memdb.NewWriter()
		ls.workQueue.Enqueue(w)
	}

	return ls
}

// RunServer runs the cache server.
func RunServer(input chan chanReq) {
	var s *luxStor
	//s.data = make(map[string]gomemcached.MCItem)
	s = initMemdb()
	for {
		go func() {
			req := <-input
			//log.Printf("Got a request: %s", req.req)
			req.res <- dispatch(req.req, s)
		}()
	}
}

func dispatch(req *gomemcached.MCRequest, s *luxStor) (rv *gomemcached.MCResponse) {
	if h, ok := handlers[req.Opcode]; ok {
		rv = h(req, s)
	} else {
		return notFound(req, s)
	}
	return
}

func notFound(req *gomemcached.MCRequest, s *luxStor) *gomemcached.MCResponse {
	var response gomemcached.MCResponse
	response.Status = gomemcached.UNKNOWN_COMMAND
	return &response
}

func handleSet(req *gomemcached.MCRequest, s *luxStor) (ret *gomemcached.MCResponse) {
	ret = &gomemcached.MCResponse{}

	/*
		item.Flags = binary.BigEndian.Uint32(req.Extras)
		item.Expiration = binary.BigEndian.Uint32(req.Extras[4:])
		item.Data = req.Body
		ret.Status = gomemcached.SUCCESS
		s.cas++
		item.Cas = s.cas
		ret.Cas = s.cas
	*/

	data := newByteItem(req.Key, req.Body)
	itm := memstore.NewItem(data)

	var worker *memstore.Writer
	var w interface{}
	var done bool

	i := 0
	for {
		w, done = s.workQueue.Dequeue()
		if done == true {
			break
		}
		i++
	}

	switch w := w.(type) {
	case *memstore.Writer:
		worker = w
	default:
		log.Printf("Not good !")
	}

	worker.Put(itm)
	s.workQueue.Enqueue(worker)

	return
}

func handleGet(req *gomemcached.MCRequest, s *luxStor) (ret *gomemcached.MCResponse) {
	ret = &gomemcached.MCResponse{}

	var worker *memstore.Writer
	var w interface{}
	var done bool

	for {
		w, done = s.workQueue.Dequeue()
		if done == true {
			break
		}
	}

	switch w := w.(type) {
	case *memstore.Writer:
		worker = w
	default:
		log.Printf("Not good !")
	}

	defer s.workQueue.Enqueue(worker)

	data := newByteItem(req.Key, nil)
	itm := memstore.NewItem(data)
	gotItm := worker.Get(itm)
	if gotItm == nil {
		ret.Status = gomemcached.KEY_ENOENT
	} else {
		bItem := byteItem(gotItm.Bytes())
		ret.Body = bItem.Value()
		ret.Status = gomemcached.SUCCESS
	}

	return
}

func handleFlush(req *gomemcached.MCRequest, s *luxStor) (ret *gomemcached.MCResponse) {
	ret = &gomemcached.MCResponse{}
	delay := binary.BigEndian.Uint32(req.Extras)
	if delay > 0 {
		log.Printf("Delay not supported (got %d)", delay)
	}
	//s.data = make(map[string]gomemcached.MCItem)
	return
}

func handleDelete(req *gomemcached.MCRequest, s *luxStor) (ret *gomemcached.MCResponse) {
	ret = &gomemcached.MCResponse{}
	//delete(s.data, string(req.Key))
	return
}
