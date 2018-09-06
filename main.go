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
	var rofsType string
	var rofsRate float64
	var rofsCmds string

	flag.StringVar(&devName, "devname", "_", "devmapper device name")
	flag.StringVar(&groupName, "groupname", "docker", "devmapper group name")
	flag.StringVar(&extentSize, "extentsize", "4M", "devmapper extent size")
	flag.StringVar(&rofsType, "rofstype", "raonfs", "filesystem type for read-only layer")
	flag.Float64Var(&rofsRate, "rofsrate", 1.2, "filesystem rate for read-only layer")
	flag.StringVar(&rofsCmds, "rofscmds", "mkraonfs.py,-s,{tars},-t,{dev}", "commands for read-only layer")
	flag.Parse()

	options := []string{}
	options = append(options, fmt.Sprintf("devname=%s", devName))
	options = append(options, fmt.Sprintf("groupname=%s", groupName))
	options = append(options, fmt.Sprintf("extentsize=%s", extentSize))
	options = append(options, fmt.Sprintf("rofstype=%s", rofsType))
	options = append(options, fmt.Sprintf("rofsrate=%f", rofsRate))
	options = append(options, fmt.Sprintf("rofscmds=%s", rofsCmds))

	d, err := NewOverlitDriver(options)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	h := graphhelper.NewHandler(d)
	h.ServeUnix(fmt.Sprintf(sockAddr, driverName), 0)
}
