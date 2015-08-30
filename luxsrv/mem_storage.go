package main

import (
	"encoding/binary"
	"fmt"
	"github.com/couchbase/gomemcached"
	"github.com/maniktaneja/luxstor/memstore"
	"github.com/maniktaneja/luxstor/replica"
	"github.com/scryner/lfreequeue"
	"log"
	"runtime"
)

type storage struct {
	data map[string]gomemcached.MCItem
	cas  uint64
}

type handler func(req *gomemcached.MCRequest, s *luxStor, id int) *gomemcached.MCResponse

var handlers = map[gomemcached.CommandCode]handler{
	gomemcached.SET:    handleSet,
	gomemcached.GET:    handleGet,
	gomemcached.DELETE: handleDelete,
	gomemcached.FLUSH:  handleFlush,
	gomemcached.STAT:   handleStat,
}

type luxStor struct {
	memdb     *memstore.MemStore
	workQueue *lfreequeue.Queue
	writers   []*memstore.Writer
}

type luxStats struct {
	Gets uint64
	Sets uint64
}

var luxstats luxStats

// init memdb
func initMemdb() *luxStor {

	runtime.GOMAXPROCS(runtime.NumCPU())

	ls := &luxStor{memdb: memstore.New()}
	ls.memdb.SetKeyComparator(byteItemKeyCompare)
	ls.workQueue = lfreequeue.NewQueue()
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		ls.writers = append(ls.writers, ls.memdb.NewWriter())
	}

	// create a queue of writers
	for i := 0; i < 128; i++ {
		w := ls.memdb.NewWriter()
		ls.workQueue.Enqueue(w)
	}

	return ls
}

func worker(id int, jobs <-chan *job) {
	for j := range jobs {
		//log.Printf("Worker id %d", id)
		j.res <- dispatch(j.req, j.s, id)
	}
}

type job struct {
	req *gomemcached.MCRequest
	res chan *gomemcached.MCResponse
	s   *luxStor
}

// RunServer runs the cache server.
func RunServer(input chan chanReq) {
	var s *luxStor
	//s.data = make(map[string]gomemcached.MCItem)
	s = initMemdb()

	jobQueue := make(chan *job, 500000)
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		go worker(i, jobQueue)
	}

	for {

		j := &job{}
		req := <-input
		//log.Printf("Got a request: %s", req.req)
		j.req = req.req
		j.res = req.res
		j.s = s
		jobQueue <- j
	}
}

func dispatch(req *gomemcached.MCRequest, s *luxStor, id int) (rv *gomemcached.MCResponse) {
	if h, ok := handlers[req.Opcode]; ok {
		rv = h(req, s, id)
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

func handleSet(req *gomemcached.MCRequest, s *luxStor, id int) (ret *gomemcached.MCResponse) {
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

	flags := binary.BigEndian.Uint32(req.Extras)
	// flags == 0 is a normal write and must be replicated
	if flags == 0 {
		if replica.IsOwner(req) != true {
			// nothing more to be done
			replica.ProxyRemoteWrite(req)
			return
		} else {
			replica.QueueRemoteWrite(req)
		}
	}

	data := newByteItem(req.Key, req.Body)
	itm := memstore.NewItem(data)

	w := s.writers[id]
	w.Put(itm)

	luxstats.Sets++

	return
}

func handleGet(req *gomemcached.MCRequest, s *luxStor, id int) (ret *gomemcached.MCResponse) {
	ret = &gomemcached.MCResponse{}

	data := newByteItem(req.Key, nil)
	itm := memstore.NewItem(data)
	w := s.writers[id]
	gotItm := w.Get(itm)
	if gotItm == nil {
		ret.Status = gomemcached.KEY_ENOENT
	} else {
		bItem := byteItem(gotItm.Bytes())
		ret.Body = bItem.Value()
		ret.Status = gomemcached.SUCCESS
	}

	luxstats.Gets++

	return
}

func handleStat(req *gomemcached.MCRequest, s *luxStor, id int) (ret *gomemcached.MCResponse) {
	ret = &gomemcached.MCResponse{}

	stats := fmt.Sprintf("Sets: %d, Gets %d", luxstats.Sets, luxstats.Gets)
	ret.Body = []byte(stats)
	ret.Status = gomemcached.SUCCESS

	return
}

func handleFlush(req *gomemcached.MCRequest, s *luxStor, id int) (ret *gomemcached.MCResponse) {
	ret = &gomemcached.MCResponse{}
	delay := binary.BigEndian.Uint32(req.Extras)
	if delay > 0 {
		log.Printf("Delay not supported (got %d)", delay)
	}
	//s.data = make(map[string]gomemcached.MCItem)
	return
}

func handleDelete(req *gomemcached.MCRequest, s *luxStor, id int) (ret *gomemcached.MCResponse) {
	ret = &gomemcached.MCResponse{}
	//delete(s.data, string(req.Key))
	return
}
