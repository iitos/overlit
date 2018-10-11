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
	var rofsOpts string
	var rofsRate float64
	var rofsSize string
	var rofsCmd0 string
	var rofsCmd1 string
	var rwfsType string
	var rwfsMkfsOpts string
	var rwfsMntOpts string
	var rwfsSize string
	var pushTar bool

	flag.StringVar(&devName, "devname", "_", "devmapper device name")
	flag.StringVar(&groupName, "groupname", "docker", "devmapper group name")
	flag.StringVar(&extentSize, "extentsize", "4M", "devmapper extent size")
	flag.StringVar(&rofsType, "rofstype", "raonfs", "filesystem type for read-only layer")
	flag.StringVar(&rofsOpts, "rofsopts", "", "filesystem options for read-only layer")
	flag.Float64Var(&rofsRate, "rofsrate", 1.8, "filesystem rate for read-only layer")
	flag.StringVar(&rofsSize, "rofssize", "0", "filesystem minimum size for read-only layer")
	flag.StringVar(&rofsCmd0, "rofscmd0", "mkraonfs.py,-s,{tars},-t,{dev}", "precommands for read-only layer")
	flag.StringVar(&rofsCmd1, "rofscmd1", "", "postcommands for read-only layer")
	flag.StringVar(&rwfsType, "rwfstype", "", "filesystem type for read-write layer")
	flag.StringVar(&rwfsMkfsOpts, "rwfsmkfsopts", "", "filesystem mkfs options for read-write layer")
	flag.StringVar(&rwfsMntOpts, "rwfsmntopts", "", "filesystem mount options for read-write layer")
	flag.StringVar(&rwfsSize, "rwfssize", "", "filesystem size for read-write layer")
	flag.BoolVar(&pushTar, "pushtar", true, "push layer as tarball")
	flag.Parse()

	options := []string{}
	options = append(options, fmt.Sprintf("devname=%s", devName))
	options = append(options, fmt.Sprintf("groupname=%s", groupName))
	options = append(options, fmt.Sprintf("extentsize=%s", extentSize))
	options = append(options, fmt.Sprintf("rofstype=%s", rofsType))
	options = append(options, fmt.Sprintf("rofsopts=%s", rofsOpts))
	options = append(options, fmt.Sprintf("rofsrate=%f", rofsRate))
	options = append(options, fmt.Sprintf("rofssize=%s", rofsSize))
	options = append(options, fmt.Sprintf("rofscmd0=%s", rofsCmd0))
	options = append(options, fmt.Sprintf("rofscmd1=%s", rofsCmd1))
	options = append(options, fmt.Sprintf("rwfstype=%s", rwfsType))
	options = append(options, fmt.Sprintf("rwfsmkfsopts=%s", rwfsMkfsOpts))
	options = append(options, fmt.Sprintf("rwfsmntopts=%s", rwfsMntOpts))
	options = append(options, fmt.Sprintf("rwfssize=%s", rwfsSize))
	options = append(options, fmt.Sprintf("pushtar=%t", pushTar))

	d, err := NewOverlitDriver(options)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	h := graphhelper.NewHandler(d)
	h.ServeUnix(fmt.Sprintf(sockAddr, driverName), 0)
}
