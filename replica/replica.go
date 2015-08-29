// get the list of owners of a key and queue writes

package replica

import (
	"hash/fnv"
	"log"
	"net"
	"strings"

	"github.com/couchbase/gomemcached"
	"github.com/maniktaneja/luxstor/clusterclient"
)

const vbucketCount = 2

var repChan chan *repItem
var connPool map[string]*connectionPool
var ipList []string

func Init() {
	repChan = make(chan *repItem, 100000)
	ipList := GetMyIp()
	if len(ipList) < 1 {
		log.Printf("Warning, iplist is empty")
	}
}

func getVbucketNode(vbid int) string {
	var vbmap string
	//Connect to cluster manager
	vbmap, _ = client.Connect("http://localhost:8091/nodes")
	nodes := strings.Split(vbmap, ",")
	return nodes[vbid]
}

func getHash(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func findShard(key string) uint32 {
	vbid := getHash(key) % vbucketCount
	return vbid
}

func GetMyIp() []string {

	ip := make([]string, 2)

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Printf("Cannot get interface address %v", err)
		return nil
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ip = append(ip, ipnet.IP.String())
			}
		}
	}

	return ip
}

type repItem struct {
	host string
	req  *gomemcached.MCRequest
}

func QueueRemoteWrite(host string, req *gomemcached.MCRequest) {

	key := req.Key
	nodeList := getVbucketNode(int(findShard(string(key))))
	nodes := strings.Split(nodeList, ";")

	if len(nodes) < 1 {
		log.Fatal("Nodelist is empty. Cannot proceed")
	}

	var remoteNode string
	// figure out which is the remote host and queue to the write to that node
	for _, node := range nodes {
		found := false
		hostname := strings.Split(node, ":")
		for _, ip := range ipList {
			if ip == hostname[0] {
				found = true
				continue
			}
		}
		if found == false {
			remoteNode = node
		}
	}

	log.Printf("Found remote node %s", remoteNode)

	ri := &repItem{host: remoteNode, req: req}
	repChan <- ri
	return
}

func drainQueue() {

	var res *gomemcached.MCResponse
	for item := range repChan {
		// get connection from pool and send the data over to the
		// remote host
		pool, ok := connPool[item.host]
		if ok == false {
			pool = newConnectionPool(item.host, 64, 128)
			connPool[item.host] = pool
		}
		cp, err := pool.Get()
		if err != nil {
			log.Printf(" Cannot get connection from pool %v", err)
			// should retry or giveup TODO
			goto done
		}

		res, err = cp.Set(0, string(item.req.Key), 0, 0, item.req.Body)
		if err != nil || res.Status != gomemcached.SUCCESS {
			log.Printf("Set failed. Error %v", err)
			goto done
		}
	done:
		pool.Return(cp)

	}
}
