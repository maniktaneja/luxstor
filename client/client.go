package main

import (
	"flag"
	"fmt"
	"github.com/couchbase/gomemcached"
	"github.com/couchbase/gomemcached/client"
	"log"
)

var server = flag.String("server", "localhost", "server URL")
var port = flag.Int("server port", 11212, "server port")

func main() {

	memServer := fmt.Sprintf("%s:%d", *server, *port)
	client, err := memcached.Connect("tcp", memServer)
	if err != nil {
		log.Printf(" Unable to connect to %v, error %v", memServer, err)
		return
	}

	log.Printf(" Connected to server %v", memServer)

	for i := 0; i < 1000000; i++ {

		res, err := client.Set(0, "test"+string(i), 0, 0, []byte("this is a test"))
		if err != nil || res.Status != gomemcached.SUCCESS {
			log.Printf("Set failed. Error %v", err)
			return
		}

		res, err = client.Get(0, "test"+string(i))
		if err != nil || res.Status != gomemcached.SUCCESS {
			log.Printf("Get failed. Error %v", err)
			return
		}
	}

	//log.Printf("Get returned %v", res)

}
