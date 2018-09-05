package main

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/willf/bitset"
)

type DmToolDevice struct {
	DevName string   `json:"devname"`
	Targets []uint64 `json:"targets"`
}

type DmTool struct {
	DevPath    string         `json:"devpath"`
	ExtentSize int64          `json:"extentsize"`
	Devices    []DmToolDevice `json:"devices"`

	blockbits *bitset.BitSet

	jsonpath string
}

func init() {
	dmUdevSetSyncSupport(1)
}

func dmToolPrepare(devpath string, extentsize int64, jsonpath string) (*DmTool, error) {
	dmtool := &DmTool{}

	fi, err := os.Stat(devpath)
	if err != nil {
		return nil, errors.New("could not open a block device")
	}

	dmtool.blockbits = bitset.New(uint(fi.Size() / extentsize))

	if jsondata, err := ioutil.ReadFile(jsonpath); err == nil {
		if err := json.Unmarshal(jsondata, &dmtool); err != nil {
			return nil, errors.New("could not parse json config")
		}

		if dmtool.DevPath == devpath && dmtool.ExtentSize == extentsize {
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

func dmToolAddDevice(dmtool *DmTool, name string) {
	var cookie uint

	task := dmTaskCreate(deviceCreate)
	dmTaskSetName(task, name)
	dmTaskAddTarget(task, 0, 4, "linear", "/dev/sdb1 2")
	dmTaskSetCookie(task, &cookie, 0)
	dmTaskRun(task)
	dmTaskDestroy(task)

	dmUdevWait(cookie)
}
