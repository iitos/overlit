package main

import (
	"os/exec"

	"github.com/pkg/errors"
)

func checkLVMAvailable() error {
	binaries := []string{"pvcreate", "pvdisplay", "pvremove", "vgcreate", "vgdisplay", "vgremove", "lvcreate", "lvdisplay", "lvremove"}

	for _, bin := range binaries {
		if _, err := exec.LookPath(bin); err != nil {
			return errors.Errorf("count not find '%s' command", bin)
		}
	}

	return nil
}

func checkBlockDeviceAvailable(devname string) (bool, error) {
	blkid, err := exec.LookPath("blkid")
	if err != nil {
		return false, errors.New("could not find blkid")
	}

	if _, err = exec.Command(blkid, devname).CombinedOutput(); err != nil {
		return false, nil
	}

	return true, nil
}

func checkLVMDeviceReady(devname string) (bool, error) {
	pvDisplay, err := exec.LookPath("pvdisplay")
	if err != nil {
		return false, errors.New("could not find pvdisplay")
	}

	if _, err = exec.Command(pvDisplay, devname).CombinedOutput(); err != nil {
		return false, nil
	}

	return true, nil
}
