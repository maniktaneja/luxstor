package main

import (
        "flag"
        "fmt"
        "github.com/couchbase/gomemcached"
        "github.com/couchbase/gomemcached/client"
        "log"
        "runtime"
        "sync"
        "time"
        "math/rand"
)

var server = flag.String("server", "localhost", "server URL")
var port = flag.Int("server port", 11212, "server port")

func init() {
    rand.Seed(time.Now().UnixNano())
}

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
        var wg sync.WaitGroup

        runtime.GOMAXPROCS(4)

        memServer := fmt.Sprintf("%s:%d", *server, *port)

        log.Printf(" Connected to server %v", memServer)

        var c []*memcached.Client

        data := RandStringRunes(512)

        for j := 0; j < runtime.GOMAXPROCS(0); j++ {
                client, err := memcached.Connect("tcp", memServer)
                if err != nil {
                        log.Printf(" Unable to connect to %v, error %v", memServer, err)
                        return
                }

                c = append(c, client)
        }
        n := 10000000
        now := time.Now()
        for j := 0; j < runtime.GOMAXPROCS(0); j++ {
                wg.Add(1)
                go func(offset int) {
                        defer wg.Done()
                        client := c[offset]

                        for i := 0; i < n; i++ {

                                res, err := client.Set(0, "test"+string(i), 0, 0, []byte(data))
                                if err != nil || res.Status != gomemcached.SUCCESS {
                                        log.Printf("Set failed. Error %v", err)
                                        return
                                }
                        }

                        for {
                                i := rand.Intn(1000000)
                                res, err := client.Get(0, "test"+string(i))
                                if err != nil || res.Status != gomemcached.SUCCESS {
                                        log.Printf("Get failed. Error %v", err)
                                        return
                                }

                                //                              log.Printf(" Time %v", elapsed)
                        }
                }(j)
        }

        wg.Wait()
        elapsed := time.Since(now)
        ops := runtime.GOMAXPROCS(0) * n
        log.Printf("sets:%d, gets:%d time_taken:%v\n", ops, ops, elapsed)

        //log.Printf("Get returned %v", res)

}

