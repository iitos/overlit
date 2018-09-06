package main

import (
	"os"
	"syscall"
	"unsafe"
)

const blkGetSize64 = 0x80081272

func getMinUint64(a, b uint64) uint64 {
	if a > b {
		return b
	}

	return a
}

func getMaxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}

	return b
}

func getDeviceSize(devpath string) uint64 {
	dev, err := os.Open(devpath)
	if err != nil {
		return 0
	}
	defer dev.Close()

	size := uint64(0)
	if _, _, err := syscall.Syscall(syscall.SYS_IOCTL, dev.Fd(), blkGetSize64, uintptr(unsafe.Pointer(&size))); err != 0 {
		return 0
	}

	return size
}
