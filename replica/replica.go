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

const OP_SET = 0x01
const OP_REP = 0x02

func Init(url string) {
	repChan = make(chan *repItem, 100000)
	ipList := GetMyIp()
	if len(ipList) < 1 {
		log.Printf("Warning, iplist is empty")
	}
	go client.RunClient(url + "/nodes")
}

func getVbucketNode(vbid int) string {
	//log.Printf(" node id %d", vbid)
	var vbmap string
	//Connect to cluster manager
	vbmap = client.GetMap()
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
	host   string
	req    *gomemcached.MCRequest
	opcode int
}

// return true if data needs to be written here
func QueueRemoteWrite(req *gomemcached.MCRequest) {

	key := req.Key
	nodeList := getVbucketNode(int(findShard(string(key))))
	nodes := strings.Split(nodeList, ";")

	if len(nodes) < 1 {
		log.Fatal("Nodelist is empty. Cannot proceed")
	}

	if len(nodes) < 2 {
		//no replica
		return
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

	log.Printf("Found replica remote node %s", remoteNode)

	ri := &repItem{host: remoteNode, req: req, opcode: OP_REP}
	repChan <- ri
	return
}

func IsOwner(req *gomemcached.MCRequest) bool {

	key := req.Key
	nodeList := getVbucketNode(int(findShard(string(key))))
	nodes := strings.Split(nodeList, ";")

	//log.Printf(" Nodes list %v key %s", nodes, string(key))
	if strings.Contains(nodes[0], "localhost") || strings.Contains(nodes[0], "127.0.0.1") || nodes[0] == "" {
		return true
	}

	if len(nodes) < 1 {
		log.Fatal("Nodelist is empty. Cannot proceed")
	}

	for _, node := range nodes {
		hostname := strings.Split(node, ":")
		for _, ip := range ipList {
			if ip == hostname[0] {
				return true
			}
		}
	}

	return false
}

// we are not the master of this node, so proxy
func ProxyRemoteWrite(req *gomemcached.MCRequest) {

	key := req.Key
	nodeList := getVbucketNode(int(findShard(string(key))))
	nodes := strings.Split(nodeList, ";")

	if len(nodes) < 1 {
		log.Fatal("Nodelist is empty. Cannot proceed")
	}

	log.Printf("Found remote proxy node %s", nodes[0])

	ri := &repItem{host: nodes[0], req: req, opcode: OP_SET}
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

		flags := 0
		cp, err := pool.Get()
		if err != nil {
			log.Printf(" Cannot get connection from pool %v", err)
			// should retry or giveup TODO
			goto done
		}

		if item.opcode == OP_REP {
			flags = 1
		}
		res, err = cp.Set(0, string(item.req.Key), flags, 0, item.req.Body)
		if err != nil || res.Status != gomemcached.SUCCESS {
			log.Printf("Set failed. Error %v", err)
			goto done
		}
	done:
		pool.Return(cp)

	}
}
