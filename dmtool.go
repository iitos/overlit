package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/willf/bitset"
)

type DmDevice struct {
	Targets []uint64 `json:"targets"`
}

type DmTool struct {
	DevPath    string               `json:"devpath"`
	ExtentSize uint64               `json:"extentsize"`
	Devices    map[string]*DmDevice `json:"devices"`

	extentbits *bitset.BitSet
	extents    uint64
	lastextent uint64

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

func findExtents(dmtool *DmTool, extents, offset uint64) (uint64, uint64, uint64) {
	start := uint64(0)
	count := uint64(0)

	for count < extents {
		index, found := dmtool.extentbits.NextClear(uint(offset + 1))
		if !found {
			break
		}
		if count == 0 {
			start = uint64(index - 1)
		} else if uint64(index) != offset+1 {
			break
		}

		dmtool.extentbits.Set(index)

		offset = uint64(index)
		count++
	}

	return start, count, offset
}

func setExtents(dmtool *DmTool, offset, count uint64) error {
	for i := uint64(0); i < count; i++ {
		dmtool.extentbits.Set(uint(offset + i + 1))
	}

	return nil
}

func clearExtents(dmtool *DmTool, offset, count uint64) error {
	for i := uint64(0); i < count; i++ {
		dmtool.extentbits.Clear(uint(offset + i + 1))
	}

	return nil
}

func attachDevice(dmtool *DmTool, devname string) error {
	var cookie uint

	device := dmtool.Devices[devname]

	multis := uint64(dmtool.ExtentSize / 512)

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

func detachDevice(dmtool *DmTool, devname string) error {
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
		return nil, errors.New("%v extent device is not available")
	}

	log.Printf("overlit: prepare (devpath = %v, devsize = %v bytes, extentsize = %v bytes)\n", devpath, devsize, extentsize)

	dmtool.extents = uint64(math.Ceil(float64(devsize / extentsize)))
	dmtool.extentbits = bitset.New(uint(dmtool.extents))

	if jsondata, err := ioutil.ReadFile(jsonpath); err == nil {
		if err := json.Unmarshal(jsondata, &dmtool); err != nil {
			return nil, errors.New("could not parse json config")
		}

		if dmtool.DevPath == devpath && dmtool.ExtentSize == extentsize {
			for devname, device := range dmtool.Devices {
				for _, target := range device.Targets {
					start, count := getTarget(target)

					setExtents(dmtool, start, count)

					dmtool.lastextent = start + count
				}

				if res := checkDevice(dmtool, devname); res == 0 {
					attachDevice(dmtool, devname)
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

func dmToolCreateDevice(dmtool *DmTool, name string, size uint64) error {
	device := &DmDevice{}

	remains := uint64(math.Ceil(float64(size / dmtool.ExtentSize)))

	for remains > 0 {
		start, count, offset := findExtents(dmtool, getMinUint64(remains, 255), dmtool.lastextent)
		if count == 0 {
			if offset >= dmtool.extents {
				dmtool.lastextent = 0
				continue
			}

			return errors.New("count not attach device")
		}

		device.Targets = append(device.Targets, start<<8|count)
		remains -= count

		dmtool.lastextent = offset
	}

	dmtool.Devices[name] = device

	return attachDevice(dmtool, name)
}

func dmToolDeleteDevice(dmtool *DmTool, name string) error {
	device := dmtool.Devices[name]

	for _, target := range device.Targets {
		start, count := getTarget(target)

		clearExtents(dmtool, start, count)
	}

	return detachDevice(dmtool, name)
}
