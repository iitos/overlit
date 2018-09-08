package main

/*
#cgo LDFLAGS: -ldevmapper
#define _GNU_SOURCE
#include <libdevmapper.h>
*/
import "C"

import (
	"reflect"
	"unsafe"
)

const (
	deviceCreate = iota
	deviceReload
	deviceRemove
	deviceRemoveAll
	deviceSuspend
	deviceResume
	deviceInfo
	deviceDeps
	deviceRename
	deviceVersion
	deviceStatus
	deviceTable
	deviceWaitevent
	deviceList
	deviceClear
	deviceMknodes
	deviceListVersions
	deviceTargetMsg
	deviceSetGeometry
)

const (
	addNodeOnResume = iota
	addNodeOnCreate
)

type (
	dmTask C.struct_dm_task
)

type DmDeps struct {
	Count  uint32
	Filler uint32
	Device []uint64
}

type DmInfo struct {
	Exists         int
	Suspended      int
	LiveTable      int
	InactiveTable  int
	OpenCount      int32
	EventNr        uint32
	Major          uint32
	Minor          uint32
	ReadOnly       int
	TargetCount    int32
	DeferredRemove int
}

func free(p *C.char) {
	C.free(unsafe.Pointer(p))
}

func dmTaskCreate(taskType int) *dmTask {
	return (*dmTask)(C.dm_task_create(C.int(taskType)))
}

func dmTaskDestroy(task *dmTask) {
	C.dm_task_destroy((*C.struct_dm_task)(task))
}

func dmTaskRun(task *dmTask) int {
	res, _ := C.dm_task_run((*C.struct_dm_task)(task))
	return int(res)
}

func dmTaskSetName(task *dmTask, name string) int {
	cname := C.CString(name)
	defer free(cname)

	return int(C.dm_task_set_name((*C.struct_dm_task)(task), cname))
}

func dmTaskSetMessage(task *dmTask, message string) int {
	cmessage := C.CString(message)
	defer free(cmessage)

	return int(C.dm_task_set_message((*C.struct_dm_task)(task), cmessage))
}

func dmTaskSetSector(task *dmTask, sector uint64) int {
	return int(C.dm_task_set_sector((*C.struct_dm_task)(task), C.uint64_t(sector)))
}

func dmTaskSetCookie(task *dmTask, cookie *uint, flags uint16) int {
	ccookie := C.uint32_t(*cookie)
	defer func() {
		*cookie = uint(ccookie)
	}()

	return int(C.dm_task_set_cookie((*C.struct_dm_task)(task), &ccookie, C.uint16_t(flags)))
}

func dmTaskSetAddNode(task *dmTask, nodeType int) int {
	return int(C.dm_task_set_add_node((*C.struct_dm_task)(task), C.dm_add_node_t(nodeType)))
}

func dmTaskSetRo(task *dmTask) int {
	return int(C.dm_task_set_ro((*C.struct_dm_task)(task)))
}

func dmTaskGetErrno(task *dmTask) int {
	return int(C.dm_task_get_errno((*C.struct_dm_task)(task)))
}

func dmTaskAddTarget(task *dmTask, start, size uint64, ttype, params string) int {
	cttype := C.CString(ttype)
	defer free(cttype)

	cparams := C.CString(params)
	defer free(cparams)

	return int(C.dm_task_add_target((*C.struct_dm_task)(task), C.uint64_t(start), C.uint64_t(size), cttype, cparams))
}

func dmTaskGetDeps(task *dmTask) *DmDeps {
	cdeps := C.dm_task_get_deps((*C.struct_dm_task)(task))
	if cdeps == nil {
		return nil
	}

	hdr := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(uintptr(unsafe.Pointer(cdeps)) + unsafe.Sizeof(*cdeps))),
		Len:  int(cdeps.count),
		Cap:  int(cdeps.count),
	}
	devices := *(*[]C.uint64_t)(unsafe.Pointer(&hdr))

	deps := &DmDeps{
		Count:  uint32(cdeps.count),
		Filler: uint32(cdeps.filler),
	}
	for _, device := range devices {
		deps.Device = append(deps.Device, uint64(device))
	}

	return deps
}

func dmTaskGetInfo(task *dmTask, info *DmInfo) int {
	cinfo := C.struct_dm_info{}
	defer func() {
		info.Exists = int(cinfo.exists)
		info.Suspended = int(cinfo.suspended)
		info.LiveTable = int(cinfo.live_table)
		info.InactiveTable = int(cinfo.inactive_table)
		info.OpenCount = int32(cinfo.open_count)
		info.EventNr = uint32(cinfo.event_nr)
		info.Major = uint32(cinfo.major)
		info.Minor = uint32(cinfo.minor)
		info.ReadOnly = int(cinfo.read_only)
		info.TargetCount = int32(cinfo.target_count)
	}()

	return int(C.dm_task_get_info((*C.struct_dm_task)(task), &cinfo))
}

func dmTaskGetDriverVersion(task *dmTask) string {
	buffer := C.malloc(128)
	defer C.free(buffer)
	res := C.dm_task_get_driver_version((*C.struct_dm_task)(task), (*C.char)(buffer), 128)
	if res == 0 {
		return ""
	}

	return C.GoString((*C.char)(buffer))
}

func dmGetNextTarget(task *dmTask, next unsafe.Pointer, start, length *uint64, target, params *string) unsafe.Pointer {
	var (
		cstart, clength C.uint64_t
		cttype, cparams *C.char
	)

	defer func() {
		*start = uint64(cstart)
		*length = uint64(clength)
		*target = C.GoString(cttype)
		*params = C.GoString(cparams)
	}()

	return C.dm_get_next_target((*C.struct_dm_task)(task), next, &cstart, &clength, &cttype, &cparams)
}

func dmUdevSetSyncSupport(syncWithUdev int) {
	C.dm_udev_set_sync_support(C.int(syncWithUdev))
}

func dmUdevGetSyncSupport() int {
	return int(C.dm_udev_get_sync_support())
}

func dmUdevWait(cookie uint) int {
	return int(C.dm_udev_wait(C.uint32_t(cookie)))
}

func dmCookieSupported() int {
	return int(C.dm_cookie_supported())
}

func dmSetDevDir(dir string) int {
	cdir := C.CString(dir)
	defer free(cdir)

	return int(C.dm_set_dev_dir(cdir))
}

func dmGetLibraryVersion(version *string) int {
	buffer := C.CString(string(make([]byte, 128)))
	defer free(buffer)
	defer func() {
		*version = C.GoString(buffer)
	}()

	return int(C.dm_get_library_version(buffer, 128))
}
