package main

import (
	"flag"
	"fmt"
	"github.com/couchbase/gomemcached"
	"github.com/couchbase/gomemcached/client"
	"log"
	"sync"
	"time"
)

var server = flag.String("server", "localhost", "server URL")
var port = flag.Int("server port", 11212, "server port")

func main() {

	var wg sync.WaitGroup

	memServer := fmt.Sprintf("%s:%d", *server, *port)
	client, err := memcached.Connect("tcp", memServer)
	if err != nil {
		log.Printf(" Unable to connect to %v, error %v", memServer, err)
		return
	}

	log.Printf(" Connected to server %v", memServer)

	for j := 0; j < 16; j++ {
		go func() {
			for i := 0; i < 1000000; i++ {

				now := time.Now()
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
				elapsed := time.Since(now)
				log.Printf(" Time %v", elapsed)
			}
			wg.Done()
		}()
		wg.Add(1)
	}

	wg.Wait()

	//log.Printf("Get returned %v", res)

}
