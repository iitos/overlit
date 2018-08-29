package main

import (
	"fmt"
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
	if _, err := exec.Command("pvdisplay", devname).CombinedOutput(); err != nil {
		return false, nil
	}

	return true, nil
}

func createLVMDevice(devname string, groupname string, extentsize int64) error {
	if out, err := exec.Command("pvcreate", "-f", devname).CombinedOutput(); err != nil {
		return errors.Wrap(err, string(out))
	}

	if out, err := exec.Command("vgcreate", "-s", fmt.Sprintf("%dM", extentsize), groupname, devname).CombinedOutput(); err != nil {
		return errors.Wrap(err, string(out))
	}

	return nil
}
