package client

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type Map struct {
	Node struct {
		ServerList string `json:"serverList"`
		LuxMap     string `json:"luxMap"`
	} `json:"nodes"`
}

var nodeMap string

func RunClient(clusterURL string) {
	for {
		resp, err := http.Get(clusterURL)
		if err != nil {
			nodeMap = ""
			time.Sleep(1 * time.Second)
			continue
		}

		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()

		var nodes Map
		json.Unmarshal([]byte(string(body)), &nodes)
		log.Printf(" got nodes %v", nodes)
		nodeMap = nodes.Node.LuxMap
		time.Sleep(1 * time.Second)
	}
}

func GetMap() string {
	return nodeMap
}
