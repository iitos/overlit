package main

import (
	"bufio"
	"os"
	"strings"

	"github.com/pkg/errors"
)

func checkFSAvailable(fstype string) error {
	f, err := os.Open("/proc/filesystems")
	if err != nil {
		return err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		if strings.Contains(s.Text(), fstype) {
			return nil
		}
	}

	return errors.Errorf("not supported %v filesystem", fstype)
}
