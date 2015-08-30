package main

import (
	"flag"
	"fmt"
	"github.com/couchbase/gomemcached"
	"github.com/couchbase/gomemcached/client"
	"log"
	"math/rand"
	"runtime"
	"sync"
	"time"
)

var server = flag.String("server", "localhost", "server URL")
var port = flag.Int("port", 11212, "server port")
var documentCount = flag.Int("documents", 100000, "no. of documents to populate")
var threadCount = flag.Int("threads", 10, "no. of goroutines to spawn")
var size = flag.Int("size", 512, "value size of documents")
var readRatio = flag.Int("ratio", 4, "read ratio vs write")

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
	var wg sync.WaitGroup

	memServer := fmt.Sprintf("%s:%d", *server, *port)

	log.Printf(" Connected to server %v", memServer)

	var c []*memcached.Client

	for j := 0; j < *threadCount; j++ {
		client, err := memcached.Connect("tcp", memServer)
		if err != nil {
			log.Printf(" Unable to connect to %v, error %v", memServer, err)
			return
		}

		c = append(c, client)
	}

	data := RandStringRunes(*size)
	now := time.Now()
	docPerThread := *documentCount / runtime.GOMAXPROCS(0)

	for j := 0; j < *threadCount; j++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			client := c[offset]

			for i := 0; i < docPerThread; i++ {
				docid := i + offset*docPerThread
				res, err := client.Set(0, "test"+string(docid), 0, 0, []byte(data))
				if err != nil || res.Status != gomemcached.SUCCESS {
					log.Printf("Set failed. Error %v", err)
					return
				}

				for k := 0; k < *readRatio; k++ {
					res, err := client.Get(0, "test"+string(docid))
					if err != nil || res.Status != gomemcached.SUCCESS {
						log.Printf("Get failed. Error %v", err)
						return
					}
				}
			}

		}(j)
	}

	wg.Wait()
	elapsed := time.Since(now)
	ops := runtime.GOMAXPROCS(0) * docPerThread
	log.Printf("sets:%d, gets:%d time_taken:%v\n", ops, ops**readRatio, elapsed)

	//log.Printf("Get returned %v", res)
}
