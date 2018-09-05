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

func checkLVMDeviceReady(devname string) (bool, error) {
	if _, err := exec.Command("blkid", devname).CombinedOutput(); err != nil {
		return false, errors.Errorf("'%s' device is not block device", devname)
	}

	if _, err := exec.Command("pvdisplay", devname).CombinedOutput(); err != nil {
		return false, nil
	}

	return true, nil
}

func createLVMDevice(devname string, groupname string, extentsize uint64) error {
	if out, err := exec.Command("pvcreate", "-f", devname).CombinedOutput(); err != nil {
		return errors.Wrap(err, string(out))
	}

	if out, err := exec.Command("vgcreate", "-s", fmt.Sprintf("%dM", extentsize), groupname, devname).CombinedOutput(); err != nil {
		return errors.Wrap(err, string(out))
	}

	return nil
}

func createLVMVolume(groupname string, volumename string, alloc string, size uint64) error {
	if out, err := exec.Command("lvcreate", groupname, "-n", volumename, "--alloc", alloc, "-L", fmt.Sprintf("%dM", size)).CombinedOutput(); err != nil {
		return errors.Wrap(err, string(out))
	}

	return nil
}

func removeLVMVolume(groupname string, volumename string) error {
	if out, err := exec.Command("lvremove", fmt.Sprintf("/dev/%s/%s", groupname, volumename)).CombinedOutput(); err != nil {
		return errors.Wrap(err, string(out))
	}

	return nil
}
