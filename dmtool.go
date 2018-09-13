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
	Targets     []uint64 `json:"targets"`
	Extents     uint64   `json:"extents"`
	FsType      string   `json:"fstype"`
	MntPath     string   `json:"mntpath"`
	Readonly    bool     `json:"readonly"`
	ExtentStart uint64   `json:"extentstart"`
	ExtentCount uint64   `json:"extentcount"`
}

type DmTool struct {
	DevPath    string               `json:"devpath"`
	ExtentSize uint64               `json:"extentsize"`
	Devices    map[string]*DmDevice `json:"devices"`

	extentbits *bitset.BitSet
	extents    uint64

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

func (d *DmTool) findExtents(start, count, extents, offset uint64) (uint64, uint64, uint64, uint64) {
	ncount := uint64(0)

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
		ncount++
	}

	return start, count, ncount, offset
}

func (d *DmTool) setExtents(offset, count uint64) error {
	for i := uint64(0); i < count; i++ {
		d.extentbits.Set(uint(offset + i + 1))
	}

	return nil
}

func (d *DmTool) clearExtents(offset, count uint64) error {
	for i := uint64(0); i < count; i++ {
		d.extentbits.Clear(uint(offset + i + 1))
	}

	return nil
}

func (d *DmTool) attachDevice(devname string) error {
	var cookie uint

	task := dmTaskCreate(deviceCreate)
	dmTaskSetName(task, devname)
	dmTaskAddTarget(task, 0, 1, "zero", "")
	dmTaskSetCookie(task, &cookie, 0)
	dmTaskRun(task)
	dmTaskDestroy(task)

	dmUdevWait(cookie)

	return nil
}

func (d *DmTool) detachDevice(devname string) error {
	var cookie uint

	task := dmTaskCreate(deviceRemove)
	dmTaskSetName(task, devname)
	dmTaskSetCookie(task, &cookie, 0)
	dmTaskRun(task)
	dmTaskDestroy(task)

	dmUdevWait(cookie)

	return nil
}

func (d *DmTool) checkDevice(devname string) int {
	info := &DmInfo{}

	task := dmTaskCreate(deviceInfo)
	dmTaskSetName(task, devname)
	dmTaskRun(task)
	dmTaskGetInfo(task, info)
	dmTaskDestroy(task)

	return info.Exists
}

func (d *DmTool) reloadDevice(devname string) error {
	device := d.Devices[devname]

	multis := uint64(d.ExtentSize / 512)

	task := dmTaskCreate(deviceReload)
	dmTaskSetName(task, devname)

	offset := uint64(0)

	for _, target := range device.Targets {
		start, count := getTarget(target)

		dmTaskAddTarget(task, offset*multis, count*multis, "linear", fmt.Sprintf("%v %v", d.DevPath, start*multis))

		offset += count
	}

	dmTaskRun(task)
	dmTaskDestroy(task)

	return nil
}

func (d *DmTool) resumeDevice(devname string) error {
	var cookie uint

	task := dmTaskCreate(deviceResume)
	dmTaskSetName(task, devname)
	dmTaskSetCookie(task, &cookie, 0)
	dmTaskRun(task)
	dmTaskDestroy(task)

	dmUdevWait(cookie)

	return nil
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

					device.ExtentStart = start
					device.ExtentCount = count

					d.setExtents(start, count)
				}

				if res := d.checkDevice(devname); res == 0 {
					d.attachDevice(devname)
				}

				if err := d.reloadDevice(devname); err != nil {
					return errors.New("could not reload device")
				}
				if err := d.resumeDevice(devname); err != nil {
					return errors.New("could not resume device")
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

func (d *DmTool) CreateDevice(name string) error {
	device := &DmDevice{}

	d.Devices[name] = device

	return d.attachDevice(name)
}

func (d *DmTool) DeleteDevice(name string) error {
	if device, ok := d.Devices[name]; ok {
		for _, target := range device.Targets {
			start, count := getTarget(target)

			d.clearExtents(start, count)
		}

		return d.detachDevice(name)
	}

	return errors.Errorf("has no %v device", name)
}

func (d *DmTool) ResizeDevice(name string, size uint64) error {
	if device, ok := d.Devices[name]; ok {
		extents := getMaxUint64(uint64(math.Ceil(float64(size/d.ExtentSize))), 1)
		if extents == device.Extents {
			return nil
		}
		if extents > device.Extents {
			remains := device.ExtentCount + (extents - device.Extents)
			estart := device.ExtentStart
			ecount := device.ExtentCount
			eoffset := estart + ecount

			for remains > 0 {
				start, count, ncount, offset := d.findExtents(estart, ecount, getMinUint64(remains, 255), eoffset)
				if ncount == 0 {
					if eoffset == 0 {
						return errors.New("could not resize device")
					}

					eoffset = 0
					continue
				}

				if ecount > 0 {
					device.Targets[len(device.Targets)-1] = start<<8 | count
				} else {
					device.Targets = append(device.Targets, start<<8|count)
				}

				device.ExtentStart = start
				device.ExtentCount = count

				remains -= count
				eoffset = offset
				estart = 0
				ecount = 0
			}

			if err := d.reloadDevice(name); err != nil {
				return err
			}
			if err := d.resumeDevice(name); err != nil {
				return err
			}

			device.Extents = extents

			return nil
		}
	}

	return errors.Errorf("has no %v device", name)
}

func (d *DmTool) HasDevice(name string) error {
	if _, ok := d.Devices[name]; ok {
		return nil
	}

	return errors.Errorf("has no %v device", name)
}

func (d *DmTool) SetDeviceFsType(name, fstype string) error {
	if device, ok := d.Devices[name]; ok {
		device.FsType = fstype

		return nil
	}

	return errors.Errorf("has no %v device", name)
}

func (d *DmTool) SetDeviceMntPath(name, mntpath string) error {
	if device, ok := d.Devices[name]; ok {
		device.MntPath = mntpath

		return nil
	}

	return errors.Errorf("has no %v device", name)
}

func (d *DmTool) SetDeviceReadonly(name string, readonly bool) error {
	if device, ok := d.Devices[name]; ok {
		device.Readonly = readonly

		return nil
	}

	return errors.Errorf("has no %v device", name)
}

func (d *DmTool) GetDeviceFsType(name string) (string, error) {
	if device, ok := d.Devices[name]; ok {
		return device.FsType, nil
	}

	return "", errors.Errorf("has no %v device", name)
}

func (d *DmTool) GetDeviceMntPath(name string) (string, error) {
	if device, ok := d.Devices[name]; ok {
		return device.MntPath, nil
	}

	return "", errors.Errorf("has no %v device", name)
}

func (d *DmTool) GetDeviceReadonly(name string) (bool, error) {
	if device, ok := d.Devices[name]; ok {
		return device.Readonly, nil
	}

	return true, errors.Errorf("has no %v device", name)
}

func NewDmTool() *DmTool {
	d := &DmTool{Devices: make(map[string]*DmDevice)}
	return d
}
