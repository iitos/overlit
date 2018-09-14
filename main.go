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
	var rofsSize string
	var rofsCmd0 string
	var rofsCmd1 string
	var pushTar bool

	flag.StringVar(&devName, "devname", "_", "devmapper device name")
	flag.StringVar(&groupName, "groupname", "docker", "devmapper group name")
	flag.StringVar(&extentSize, "extentsize", "4M", "devmapper extent size")
	flag.StringVar(&rofsType, "rofstype", "raonfs", "filesystem type for read-only layer")
	flag.Float64Var(&rofsRate, "rofsrate", 1.2, "filesystem rate for read-only layer")
	flag.StringVar(&rofsSize, "rofssize", "0", "filesystem minimum size for read-only layer")
	flag.StringVar(&rofsCmd0, "rofscmd0", "mkraonfs.py,-s,{tars},-t,{dev}", "precommands for read-only layer")
	flag.StringVar(&rofsCmd1, "rofscmd1", "", "postcommands for read-only layer")
	flag.BoolVar(&pushTar, "pushtar", true, "push layer as tarball")
	flag.Parse()

	options := []string{}
	options = append(options, fmt.Sprintf("devname=%s", devName))
	options = append(options, fmt.Sprintf("groupname=%s", groupName))
	options = append(options, fmt.Sprintf("extentsize=%s", extentSize))
	options = append(options, fmt.Sprintf("rofstype=%s", rofsType))
	options = append(options, fmt.Sprintf("rofsrate=%f", rofsRate))
	options = append(options, fmt.Sprintf("rofssize=%s", rofsSize))
	options = append(options, fmt.Sprintf("rofscmd0=%s", rofsCmd0))
	options = append(options, fmt.Sprintf("rofscmd1=%s", rofsCmd1))
	options = append(options, fmt.Sprintf("pushtar=%t", pushTar))

	d, err := NewOverlitDriver(options)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	h := graphhelper.NewHandler(d)
	h.ServeUnix(fmt.Sprintf(sockAddr, driverName), 0)
}
