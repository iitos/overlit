package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/willf/bitset"
)

const (
	blkGetSize64 = 0x80081272
)

type DmDevice struct {
	Targets []uint64 `json:"targets"`
}

type DmTool struct {
	DevPath    string               `json:"devpath"`
	ExtentSize uint64               `json:"extentsize"`
	Devices    map[string]*DmDevice `json:"devices"`

	blockbits *bitset.BitSet

	jsonpath string
}

func init() {
	dmUdevSetSyncSupport(1)
}

func getTarget(target uint64) (start, count uint64) {
	start = uint64(target >> 8)
	count = uint64(target & 0xff)

	return
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

func findExtents(dmtool *DmTool, blocks, offset uint64) (uint64, uint64) {
	start := uint64(0)
	count := uint64(0)

	for count < blocks {
		index, found := dmtool.blockbits.NextClear(uint(offset + 1))
		if !found {
			break
		}
		if count == 0 {
			start = uint64(index - 1)
		} else if uint64(index) != offset+1 {
			break
		}

		dmtool.blockbits.Set(index)

		offset = uint64(index)
		count++
	}

	return start, count
}

func setExtents(dmtool *DmTool, offset, count uint64) error {
	for i := uint64(0); i < count; i++ {
		dmtool.blockbits.Set(uint(offset + i + 1))
	}

	return nil
}

func clearExtents(dmtool *DmTool, offset, count uint64) error {
	for i := uint64(0); i < count; i++ {
		dmtool.blockbits.Clear(uint(offset + i + 1))
	}

	return nil
}

func addDevice(dmtool *DmTool, devname string) error {
	var cookie uint

	device := dmtool.Devices[devname]

	multis := uint64(dmtool.ExtentSize * 1024 * 1024 / 512)

	task := dmTaskCreate(deviceCreate)
	dmTaskSetName(task, devname)

	offset := uint64(0)

	for _, target := range device.Targets {
		start, count := getTarget(target)

		dmTaskAddTarget(task, offset*multis, count*multis, "linear", fmt.Sprintf("%v %v", dmtool.DevPath, start*multis))

		offset += count
	}

	dmTaskSetCookie(task, &cookie, 0)
	dmTaskRun(task)
	dmTaskDestroy(task)

	dmUdevWait(cookie)

	return nil
}

func deleteDevice(dmtool *DmTool, devname string) error {
	var cookie uint

	task := dmTaskCreate(deviceRemove)
	dmTaskSetName(task, devname)
	dmTaskSetCookie(task, &cookie, 0)
	dmTaskRun(task)
	dmTaskDestroy(task)

	dmUdevWait(cookie)

	return nil
}

func checkDevice(dmtool *DmTool, devname string) int {
	info := &DmInfo{}

	task := dmTaskCreate(deviceInfo)
	dmTaskSetName(task, devname)
	dmTaskRun(task)
	dmTaskGetInfo(task, info)
	dmTaskDestroy(task)

	return info.Exists
}

func dmToolSetup(devpath string, extentsize uint64, jsonpath string) (*DmTool, error) {
	dmtool := &DmTool{Devices: make(map[string]*DmDevice)}

	devsize := getDeviceSize(devpath)
	if devsize == 0 {
		return nil, errors.New("%v block device is not available")
	}

	log.Printf("overlit: prepare (devpath = %v, devsize = %v mbytes, extentsize = %v mbytes)\n", devpath, devsize/1024/1024, extentsize)

	dmtool.blockbits = bitset.New(uint(devsize) / uint(extentsize*1024*1024))

	if jsondata, err := ioutil.ReadFile(jsonpath); err == nil {
		if err := json.Unmarshal(jsondata, &dmtool); err != nil {
			return nil, errors.New("could not parse json config")
		}

		if dmtool.DevPath == devpath && dmtool.ExtentSize == extentsize {
			for devname, device := range dmtool.Devices {
				for _, target := range device.Targets {
					start, count := getTarget(target)

					setExtents(dmtool, start, count)
				}

				if res := checkDevice(dmtool, devname); res == 0 {
					addDevice(dmtool, devname)
				}
			}
		}
	}

	dmtool.DevPath = devpath
	dmtool.ExtentSize = extentsize

	dmtool.jsonpath = jsonpath

	return dmtool, nil
}

func dmToolCleanup(dmtool *DmTool) {
	dmToolFlush(dmtool)
}

func dmToolFlush(dmtool *DmTool) error {
	jsondata, err := json.Marshal(dmtool)
	if err != nil {
		return errors.New("could not encode json config")
	}

	tmpfile, err := ioutil.TempFile(filepath.Dir(dmtool.jsonpath), ".tmp")
	if err != nil {
		return errors.New("could not create temp file for json config")
	}

	n, err := tmpfile.Write(jsondata)
	if err != nil {
		return errors.New("could not write json config to temp file")
	}
	if n < len(jsondata) {
		return io.ErrShortWrite
	}
	if err := tmpfile.Sync(); err != nil {
		return errors.New("could not sync temp file")
	}
	if err := tmpfile.Close(); err != nil {
		return errors.New("could not close temp file")
	}
	if err := os.Rename(tmpfile.Name(), dmtool.jsonpath); err != nil {
		return errors.New("could not commit json config")
	}

	return nil
}

func dmToolAddDevice(dmtool *DmTool, name string, size uint64) error {
	device := &DmDevice{}

	remains := size / (dmtool.ExtentSize * 1024 * 1024)
	offset := uint64(0)
	start := uint64(0)

	for remains > 0 {
		start, count := findExtents(dmtool, minUint64(remains, 255), start)
		if count == 0 {
			return errors.New("count not add device")
		}

		device.Targets = append(device.Targets, start<<8|count)

		remains -= count
		offset += count
		start = start + count
	}

	dmtool.Devices[name] = device

	return addDevice(dmtool, name)
}

func dmToolDeleteDevice(dmtool *DmTool, name string) error {
	device := dmtool.Devices[name]

	for _, target := range device.Targets {
		start, count := getTarget(target)

		clearExtents(dmtool, start, count)
	}

	return deleteDevice(dmtool, name)
}
