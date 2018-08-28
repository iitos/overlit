package main

import (
	graphhelper "github.com/docker/go-plugins-helpers/graphdriver"
)

const (
	sockAddr = "/run/docker/plugins/overlit.sock"
)

func main() {
	d, _ := newOverlitDriver()
	h := graphhelper.NewHandler(d)
	h.ServeUnix(sockAddr, 0)
}
