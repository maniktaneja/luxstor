package main

import (
	"flag"
	"fmt"
	"github.com/couchbase/gomemcached/client"
	"log"
)

var server = flag.String("server", "localhost", "server URL")
var port = flag.Int("server port", 11212, "server port")

func main() {

	memServer := fmt.Sprintf("%s:%d", *server, *port)

	log.Printf(" Connected to server %v", memServer)

	client, err := memcached.Connect("tcp", memServer)
	if err != nil {
		log.Printf(" Unable to connect to %v, error %v", memServer, err)
		return
	}

	res, err := client.GetAndTouch(0, "", 0)
	if err != nil {
		log.Printf("Stats failed. Error %v", err)
		return
	}
	log.Printf("Stats %v", res)

	//log.Printf("Get returned %v", res)
}
