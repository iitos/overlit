package main

import (
	"flag"
	"fmt"
)

import (
	graphhelper "github.com/docker/go-plugins-helpers/graphdriver"
)

const (
	sockAddr = "/run/docker/plugins/overlit.sock"
)

func main() {
	var devname string

	flag.StringVar(&devname, "devname", "", "devmapper device name")
	flag.Parse()

	options := []string{}

	if devname != "" {
		options = append(options, fmt.Sprintf("devname=%s", devname))
	}

	d, _ := newOverlitDriver(options)
	h := graphhelper.NewHandler(d)
	h.ServeUnix(sockAddr, 0)
}
