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
	var devname string
	var groupname string
	var extentsize int

	flag.StringVar(&devname, "devname", "_", "devmapper device name")
	flag.StringVar(&groupname, "groupname", "docker", "devmapper group name")
	flag.IntVar(&extentsize, "extentsize", 4, "devmapper extent size (mbytes)")
	flag.Parse()

	options := []string{}
	options = append(options, fmt.Sprintf("devname=%s", devname))
	options = append(options, fmt.Sprintf("groupname=%s", groupname))
	options = append(options, fmt.Sprintf("extentsize=%d", extentsize))

	d, err := newOverlitDriver(options)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	h := graphhelper.NewHandler(d)
	h.ServeUnix(sockAddr, 0)
}
