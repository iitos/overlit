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
	sockAddr = "/run/docker/plugins/%v.sock"
)

func main() {
	var devName string
	var groupName string
	var extentSize string

	flag.StringVar(&devName, "devname", "_", "devmapper device name")
	flag.StringVar(&groupName, "groupname", "docker", "devmapper group name")
	flag.StringVar(&extentSize, "extentsize", "4M", "devmapper extent size")
	flag.Parse()

	options := []string{}
	options = append(options, fmt.Sprintf("devname=%s", devName))
	options = append(options, fmt.Sprintf("groupname=%s", groupName))
	options = append(options, fmt.Sprintf("extentsize=%s", extentSize))

	d, err := NewOverlitDriver(options)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	h := graphhelper.NewHandler(d)
	h.ServeUnix(fmt.Sprintf(sockAddr, driverName), 0)
}
