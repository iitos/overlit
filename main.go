package main

import (
	graphhelper "github.com/docker/go-plugins-helpers/graphdriver"
	overlitdriver "github.com/iitos/overlit/driver"
)

const (
	sockAddr = "/run/docker/plugins/overlit.sock"
)

func main() {
	d, _ := overlitdriver.NewOverlitDriver()
	h := graphhelper.NewHandler(d)
	h.ServeUnix(sockAddr, 0)
}
