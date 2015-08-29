package main

import (
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/maniktaneja/luxstor/clusterclient"
)

const vbucketCount = 2

func getVbucketNode(vbid int) []string {
	var vbmap string
	//Connect to cluster manager
	vbmap, _ = client.RunClient("http://localhost:8091/nodes")
	nodes := strings.Split(vbmap, ",")
	return strings.Split(nodes[vbid], ";")
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

func main() {
	entry := getVbucketNode(1)
	fmt.Println(entry)
}