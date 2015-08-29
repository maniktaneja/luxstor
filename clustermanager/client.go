package client

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
)

type Map struct {
	Node struct {
		ServerList string `json:"serverList"`
		LuxMap string `json:"luxMap"`
	} `json:"nodes"`
}

func Connect(clusterURL string) (string, error) {

	resp, err := http.Get(clusterURL)
	if err != nil {
		return "", err
	}

	//Need to handle this in-case cluster manager dies
	defer resp.Body.Close()
	
	body, err := ioutil.ReadAll(resp.Body)

	var nodes Map	
	json.Unmarshal([]byte(string(body)), &nodes)
	vbmap := nodes.Node.LuxMap

	return vbmap, nil
}

