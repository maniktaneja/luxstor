package client

import (
	"encoding/json"
	"io/ioutil"
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
		}

		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)

		var nodes Map
		json.Unmarshal([]byte(string(body)), &nodes)
		nodeMap = nodes.Node.LuxMap
		time.Sleep(1 * time.Second)
	}
}

func GetMap() string {
	return nodeMap
}
