package main

import (
	"os/exec"

	"github.com/pkg/errors"
)

func checkLVMReady() error {
	binaries := []string{"pvcreate", "pvdisplay", "pvremove", "vgcreate", "vgdisplay", "vgremove", "lvcreate", "lvdisplay", "lvremove"}

	for _, bin := range binaries {
		if _, err := exec.LookPath(bin); err != nil {
			return errors.Errorf("count not find '%s' command", bin)
		}
	}

	return nil
}

func checkLVMDeviceReady(devname string) error {
	pvDisplay, err := exec.LookPath("pvdisplay")
	if err != nil {
		return errors.New("could not find pvdisplay")
	}

	_, err = exec.Command(pvDisplay, devname).CombinedOutput()
	if err != nil {
		return nil
	}

	return nil
}
