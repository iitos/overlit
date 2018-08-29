package main

import (
	"bufio"
	"os"
	"os/exec"

	"github.com/pkg/errors"
)

func checkOverlayFSAvailable() error {
	exec.Command("modprobe", "overlay").Run()

	f, err := os.Open("/proc/filesystems")
	if err != nil {
		return err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		if s.Text() == "nodev\toverlay" {
			return nil
		}
	}

	return errors.New("not supported overlay filesystem")
}
