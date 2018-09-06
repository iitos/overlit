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

func findExtents(d *DmTool, extents, offset uint64) (uint64, uint64, uint64) {
	start := uint64(0)
	count := uint64(0)

	for count < extents {
		index, found := d.extentbits.NextClear(uint(offset + 1))
		if !found {
			break
		}
		if count == 0 {
			start = uint64(index - 1)
		} else if uint64(index) != offset+1 {
			break
		}

		d.extentbits.Set(index)

		offset = uint64(index)
		count++
	}

	return start, count, offset
}

func setExtents(d *DmTool, offset, count uint64) error {
	for i := uint64(0); i < count; i++ {
		d.extentbits.Set(uint(offset + i + 1))
	}

	return nil
}

func clearExtents(d *DmTool, offset, count uint64) error {
	for i := uint64(0); i < count; i++ {
		d.extentbits.Clear(uint(offset + i + 1))
	}

	return nil
}

func attachDevice(d *DmTool, devname string) error {
	var cookie uint

	device := d.Devices[devname]

	multis := uint64(d.ExtentSize / 512)

	task := dmTaskCreate(deviceCreate)
	dmTaskSetName(task, devname)

	offset := uint64(0)

	for _, target := range device.Targets {
		start, count := getTarget(target)

		dmTaskAddTarget(task, offset*multis, count*multis, "linear", fmt.Sprintf("%v %v", d.DevPath, start*multis))

		offset += count
	}

	dmTaskSetCookie(task, &cookie, 0)
	dmTaskRun(task)
	dmTaskDestroy(task)

	dmUdevWait(cookie)

	return nil
}

func detachDevice(d *DmTool, devname string) error {
	var cookie uint

	task := dmTaskCreate(deviceRemove)
	dmTaskSetName(task, devname)
	dmTaskSetCookie(task, &cookie, 0)
	dmTaskRun(task)
	dmTaskDestroy(task)

	dmUdevWait(cookie)

	return nil
}

func checkDevice(d *DmTool, devname string) int {
	info := &DmInfo{}

	task := dmTaskCreate(deviceInfo)
	dmTaskSetName(task, devname)
	dmTaskRun(task)
	dmTaskGetInfo(task, info)
	dmTaskDestroy(task)

	return info.Exists
}

func (d *DmTool) Setup(devpath string, extentsize uint64, jsonpath string) error {
	devsize := getDeviceSize(devpath)
	if devsize == 0 {
		return errors.New("%v extent device is not available")
	}

	log.Printf("overlit: prepare (devpath = %v, devsize = %v bytes, extentsize = %v bytes)\n", devpath, devsize, extentsize)

	d.extents = uint64(math.Ceil(float64(devsize / extentsize)))
	d.extentbits = bitset.New(uint(d.extents))

	if jsondata, err := ioutil.ReadFile(jsonpath); err == nil {
		if err := json.Unmarshal(jsondata, &d); err != nil {
			return errors.New("could not parse json config")
		}

		if d.DevPath == devpath && d.ExtentSize == extentsize {
			for devname, device := range d.Devices {
				for _, target := range device.Targets {
					start, count := getTarget(target)

					setExtents(d, start, count)

					d.lastextent = start + count
				}

				if res := checkDevice(d, devname); res == 0 {
					attachDevice(d, devname)
				}
			}
		}
	}

	d.DevPath = devpath
	d.ExtentSize = extentsize

	d.jsonpath = jsonpath

	return nil
}

func (d *DmTool) Cleanup() {
	d.Flush()
}

func (d *DmTool) Flush() error {
	jsondata, err := json.Marshal(d)
	if err != nil {
		return errors.New("could not encode json config")
	}

	tmpfile, err := ioutil.TempFile(filepath.Dir(d.jsonpath), ".tmp")
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
	if err := os.Rename(tmpfile.Name(), d.jsonpath); err != nil {
		return errors.New("could not commit json config")
	}

	return nil
}

func (d *DmTool) CreateDevice(name string, size uint64) error {
	device := &DmDevice{}

	remains := uint64(math.Ceil(float64(size / d.ExtentSize)))

	for remains > 0 {
		start, count, offset := findExtents(d, getMinUint64(remains, 255), d.lastextent)
		if count == 0 {
			if offset >= d.extents {
				d.lastextent = 0
				continue
			}

			return errors.New("count not attach device")
		}

		device.Targets = append(device.Targets, start<<8|count)
		remains -= count

		d.lastextent = offset
	}

	d.Devices[name] = device

	return attachDevice(d, name)
}

func (d *DmTool) DeleteDevice(name string) error {
	device := d.Devices[name]

	for _, target := range device.Targets {
		start, count := getTarget(target)

		clearExtents(d, start, count)
	}

	return detachDevice(d, name)
}

func NewDmTool() *DmTool {
	d := &DmTool{Devices: make(map[string]*DmDevice)}
	return d
}
