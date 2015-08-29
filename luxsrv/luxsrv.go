package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/couchbase/gomemcached"
	"github.com/couchbase/gomemcached/server"
	"github.com/maniktaneja/luxstor/replica"
)

var port = flag.Int("port", 11212, "Port on which to listen")
var clusterMgr = flag.String("clusterMgr", "http://localhost:8091/", "Cluster manager url")

type chanReq struct {
	req *gomemcached.MCRequest
	res chan *gomemcached.MCResponse
}

type reqHandler struct {
	ch chan chanReq
}

func (rh *reqHandler) HandleMessage(w io.Writer, req *gomemcached.MCRequest) *gomemcached.MCResponse {
	cr := chanReq{
		req,
		make(chan *gomemcached.MCResponse),
	}

	rh.ch <- cr
	return <-cr.res
}

func connectionHandler(s net.Conn, h memcached.RequestHandler) {
	// Explicitly ignoring errors since they all result in the
	// client getting hung up on and many are common.
	_ = memcached.HandleIO(s, h)
}

func waitForConnections(ls net.Listener) {
	reqChannel := make(chan chanReq, 100000)

	go RunServer(reqChannel)
	handler := &reqHandler{reqChannel}

	log.Printf("Listening on port %d", *port)
	for {
		s, e := ls.Accept()
		if e == nil {
			//log.Printf("Got a connection from %v", s.RemoteAddr())
			go connectionHandler(s, handler)
		} else {
			log.Printf("Error accepting from %s", ls)
		}
	}
}

func main() {
	flag.Parse()
	replica.Init(*clusterMgr)
	ls, e := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if e != nil {
		log.Fatalf("Got an error:  %s", e)
	}

	waitForConnections(ls)
}
