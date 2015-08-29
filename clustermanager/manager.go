package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/couchbaselabs/clog"
)

const vbucketCount = 2

type NodeStatus struct {
	status  string
	retries int
}

type vbucketMap struct {
	id    int
	nodes []string
}

var (
	address   string
	port      int
	logPath   string
	hosts     string
	nodes     = make(map[string]NodeStatus)
	bucketMap = make(map[string]string)
)

func init() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.StringVar(&address, "address", "", "Address to listen on, Default is to all")
	flag.IntVar(&port, "port", 8091, "Port to listen on. Default is 8091")
	flag.StringVar(&logPath, "path", "manager", "cluster manager logging dir")
	flag.StringVar(&hosts, "host", "localhost:11212", "nodes to manage")
	flag.Parse()

}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func Nodes(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(200)

	//onlineNodes := make([]string, 0)
	//onlineNodes := make(map[string]string)
	//for node, _ := range bucketMap {
	//	onlineNodes = append(onlineNodes, node)
	//}

	//oNodes, _ := json.Marshal(onlineNodes)
	oNodes, _ := json.Marshal(bucketMap)

	fmt.Fprintf(w, fmt.Sprintf("{\"nodes\":%s}", oNodes))
}

func main() {

	log.Printf("listening on %s:%d\n", address, port)
	log.Printf("cluster manager Path: %s\n", logPath)

	for _, host := range strings.Split(hosts, ",") {

		//conn, err := net.Dial("tcp", host)
		_, err := net.Dial("tcp", host)
		//defer conn.Close()

		nodes[host] = NodeStatus{status: "up", retries: 0}
		if err != nil {
			clog.Error(err)
			nodes[host] = NodeStatus{status: "down", retries: 0}
			break
		}
	}

	nodeCount := len(nodes)

	servers := make([]string, 0, nodeCount)
	for n := range nodes {
		servers = append(servers, n)
	}
	sort.Strings(servers)

	fmt.Printf("%#v\n", nodes)

	vbmap := make(map[int][]string)

	//TODO - fix it
	for i := 0; i < nodeCount; i++ {
		if hash(servers[i])%2 == 0 {
			vbmap[0] = append(vbmap[0], servers[i])
		} else {
			vbmap[1] = append(vbmap[1], servers[i])
		}
	}

	bucketMap["serverList"] = hosts
	//bucketMap["luxmap"] = strings.Join(vbmap[0], ";") + "," + strings.Join(vbmap[1], ";")
	bucketMap["luxmap"] = "0:" + strings.Join(vbmap[0], ";") + ", 1:" + strings.Join(vbmap[1], ";")

	fmt.Println("vbmap:", bucketMap)

	//Polling nodes, needs cleanup
	go func() {
		for {
			for node, _ := range nodes {
				_, err := net.Dial("tcp", node)
				//conn, err := net.Dial("tcp", node)
				//defer conn.Close()

				if err != nil {
					clog.Error(err)
					fmt.Println("retry count:", nodes[node].retries, " node:", node)
					retryCount := nodes[node].retries + 1

					if retryCount <= 3 {
						nodes[node] = NodeStatus{status: "down", retries: retryCount}
					} else {
						delete(nodes, node)
					}
					break
				} else {
					nodes[node] = NodeStatus{status: "up", retries: 0}
				}
			}

			nodeCount := len(nodes)
			servers := make([]string, 0, nodeCount)
			for n := range nodes {
				servers = append(servers, n)
			}
			sort.Strings(servers)
			fmt.Printf("%#v\n", nodes)

			vbmap = make(map[int][]string)

			//TODO - fix it
			for i := 0; i < nodeCount; i++ {
				if hash(servers[i])%2 == 0 {
					vbmap[0] = append(vbmap[0], servers[i])
				} else {
					vbmap[1] = append(vbmap[1], servers[i])
				}
			}

			bucketMap["serverList"] = strings.Join(servers, ",")
			//bucketMap["luxmap"] = strings.Join(vbmap[0], ";") + "," + strings.Join(vbmap[1], ";")
			bucketMap["luxmap"] = "0:" + strings.Join(vbmap[0], ";") + ", 1:" + strings.Join(vbmap[1], ";")
			fmt.Printf("%#v\n", nodes)
			time.Sleep(time.Second)
		}
	}()

	http.HandleFunc("/nodes", Nodes)

	err := http.ListenAndServe(fmt.Sprintf("%s:%d", address, port), nil)
	if err != nil {
		log.Fatalf("Failed to start cluster manager: %v", err)
	}
}
