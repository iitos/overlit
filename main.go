package main

import (
	"flag"
	"fmt"
	"os"
)

import (
	graphhelper "github.com/docker/go-plugins-helpers/graphdriver"
)

const (
	sockAddr = "/run/docker/plugins/overlit.sock"
)

func main() {
	var devName string
	var groupName string
	var extentSize int

	flag.StringVar(&devName, "devname", "_", "devmapper device name")
	flag.StringVar(&groupName, "groupname", "docker", "devmapper group name")
	flag.IntVar(&extentSize, "extentsize", 4, "devmapper extent size (mbytes)")
	flag.Parse()

	options := []string{}
	options = append(options, fmt.Sprintf("devname=%s", devName))
	options = append(options, fmt.Sprintf("groupname=%s", groupName))
	options = append(options, fmt.Sprintf("extentsize=%d", extentSize))

	d, err := newOverlitDriver(options)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	h := graphhelper.NewHandler(d)
	h.ServeUnix(sockAddr, 0)
}
