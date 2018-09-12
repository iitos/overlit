package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"

	"github.com/docker/docker/daemon/graphdriver"
	"github.com/docker/docker/pkg/archive"
	"github.com/docker/docker/pkg/containerfs"
	"github.com/docker/docker/pkg/directory"
	"github.com/docker/docker/pkg/idtools"
	"github.com/docker/docker/pkg/locker"
	"github.com/docker/docker/pkg/mount"
	"github.com/docker/docker/pkg/parsers"
	"github.com/docker/docker/pkg/system"
	"github.com/docker/go-units"
	"github.com/pkg/errors"

	gdhelper "github.com/docker/go-plugins-helpers/graphdriver"
	rsystem "github.com/opencontainers/runc/libcontainer/system"

	"github.com/opencontainers/selinux/go-selinux/label"
	"golang.org/x/sys/unix"
)

const (
	driverName = "overlit"
	linkDir    = "l"
	diffDir    = "diff"
	tarsDir    = "tars"
	linkFile   = "link"
	lowerFile  = "lower"
	workDir    = "work"
	mergedDir  = "merged"
	configFile = "dmtool.json"
	maxDepth   = 128
	idLength   = 26
)

var pageSize int = 4096

type overlitOptions struct {
	DevName    string
	GroupName  string
	ExtentSize uint64
	RofsType   string
	RofsRate   float64
	RofsCmd0   string
	RofsCmd1   string
}

type overlitDriver struct {
	options overlitOptions

	home string

	uidMaps []idtools.IDMap
	gidMaps []idtools.IDMap

	ctr    *graphdriver.RefCounter
	locker *locker.Locker

	dmtool *DmTool
}

func init() {
	pageSize := unix.Getpagesize()

	if pageSize > 4096 {
		pageSize = 4096
	}
}

func parseOptions(options []string) (*overlitOptions, error) {
	opts := &overlitOptions{}
	for _, opt := range options {
		key, val, err := parsers.ParseKeyValueOpt(opt)
		if err != nil {
			return nil, err
		}
		key = strings.ToLower(key)
		switch key {
		case "devname":
			opts.DevName = val
		case "groupname":
			opts.GroupName = val
		case "extentsize":
			size, _ := units.RAMInBytes(val)
			opts.ExtentSize = uint64(size)
		case "rofstype":
			opts.RofsType = val
		case "rofsrate":
			opts.RofsRate, _ = strconv.ParseFloat(val, 64)
		case "rofscmd0":
			opts.RofsCmd0 = val
		case "rofscmd1":
			opts.RofsCmd1 = val
		default:
			return nil, fmt.Errorf("overlit: Unknown option (%s = %s)", key, val)
		}
	}

	return opts, nil
}

func parseRWFSOptions(options map[string]string) (fstype string, fssize uint64, rerr error) {
	for key, val := range options {
		key = strings.ToLower(key)
		switch key {
		case "rwfstype":
			// Check if read-write filesystem is available
			if err := checkFSAvailable(val); err != nil {
				return "", 0, err
			}
			fstype = val
		case "rwfssize":
			size, _ := units.RAMInBytes(val)
			fssize = uint64(size)
		default:
			return "", 0, errors.Errorf("not supported option (%s = %s)", key, val)
		}
	}

	return
}

func getGDHelperChanges(_changes []archive.Change) ([]gdhelper.Change, error) {
	changes := make([]gdhelper.Change, len(_changes))

	for i, _change := range _changes {
		changes[i] = gdhelper.Change{
			Path: _change.Path,
			Kind: gdhelper.ChangeKind(_change.Kind),
		}
	}

	return changes, nil
}

func getAbsPaths(dir string, _paths []string) []string {
	paths := make([]string, len(_paths))

	for i, s := range _paths {
		paths[i] = path.Join(dir, s)
	}

	return paths
}

func (d *overlitDriver) getHomePath(id string) string {
	return path.Join(d.home, id)
}

func (d *overlitDriver) getDiffPath(home string) string {
	return path.Join(home, diffDir)
}

func (d *overlitDriver) getTarsPath(home string) string {
	return path.Join(home, tarsDir)
}

func (d *overlitDriver) getLinkPath(home string) string {
	return path.Join(home, linkFile)
}

func (d *overlitDriver) getLowerPath(home string) string {
	return path.Join(home, lowerFile)
}

func (d *overlitDriver) getWorkPath(home string) string {
	return path.Join(home, workDir)
}

func (d *overlitDriver) getMergedPath(home string) string {
	return path.Join(home, mergedDir)
}

func (d *overlitDriver) getDevPath(id string) string {
	return path.Join("/dev/mapper", id)
}

func (d *overlitDriver) getRootIdentity() (idtools.Identity, int, int, error) {
	rootUID, rootGID, err := idtools.GetRootUIDGID(d.uidMaps, d.gidMaps)
	if err != nil {
		return idtools.Identity{}, 0, 0, err
	}
	root := idtools.Identity{UID: rootUID, GID: rootGID}

	return root, rootUID, rootGID, nil
}

func (d *overlitDriver) execCommands(cmds string) error {
	for _, cmd := range strings.Split(cmds, ":") {
		args := strings.Split(cmd, ",")
		if len(args[0]) == 0 {
			continue
		}

		if err := exec.Command(args[0], args[1:]...).Run(); err != nil {
			return err
		}
	}

	return nil
}

func (d *overlitDriver) createHomeDir(id, parent string, root idtools.Identity) error {
	dir := d.getHomePath(id)

	if err := idtools.MkdirAllAndChown(path.Dir(dir), 0700, root); err != nil {
		return err
	}
	if err := idtools.MkdirAndChown(dir, 0700, root); err != nil {
		return err
	}

	return nil
}

func (d *overlitDriver) createSubDir(id, parent string, root idtools.Identity) error {
	dir := d.getHomePath(id)
	tarsPath := d.getTarsPath(dir)
	diffPath := d.getDiffPath(dir)
	linkPath := d.getLinkPath(dir)
	workPath := d.getWorkPath(dir)

	lid := generateID(idLength)

	if err := idtools.MkdirAndChown(diffPath, 0755, root); err != nil {
		return err
	}

	if err := os.Symlink(path.Join("..", id, diffDir), path.Join(d.home, linkDir, lid)); err != nil {
		return err
	}

	if err := idtools.MkdirAndChown(tarsPath, 0755, root); err != nil {
		return err
	}

	if err := ioutil.WriteFile(linkPath, []byte(lid), 0644); err != nil {
		return err
	}

	if parent == "" {
		return nil
	}

	if err := idtools.MkdirAndChown(workPath, 0700, root); err != nil {
		return err
	}

	pdir := d.getHomePath(parent)

	plink, err := ioutil.ReadFile(d.getLinkPath(pdir))
	if err != nil {
		return err
	}
	lowers := []string{path.Join(linkDir, string(plink))}

	plower, err := ioutil.ReadFile(d.getLowerPath(pdir))
	if err == nil {
		plowers := strings.Split(string(plower), ":")
		lowers = append(lowers, plowers...)
	}
	if len(lowers) > maxDepth {
		return errors.New("max depth exceeded")
	}

	if len(lowers) > 0 {
		if err := ioutil.WriteFile(d.getLowerPath(dir), []byte(strings.Join(lowers, ":")), 0666); err != nil {
			return err
		}
	}

	return nil
}

func (d *overlitDriver) Init(home string, options []string, uidMaps, gidMaps []idtools.IDMap) error {
	log.Printf("overlit: init (home = %s)\n", home)

	d.home = home
	d.uidMaps = uidMaps
	d.gidMaps = gidMaps
	d.ctr = graphdriver.NewRefCounter(graphdriver.NewFsChecker(graphdriver.FsMagicOverlay))
	d.locker = locker.New()

	root, _, _, err := d.getRootIdentity()
	if err != nil {
		return err
	}
	if err := idtools.MkdirAllAndChown(path.Join(home, linkDir), 0700, root); err != nil {
		return err
	}

	if err := d.dmtool.Setup(d.options.DevName, d.options.ExtentSize, fmt.Sprintf("%v/%v", d.home, configFile)); err != nil {
		return err
	}

	for devname, device := range d.dmtool.Devices {
		devPath := d.getDevPath(devname)

		if err := unix.Mount(devPath, device.MntPath, device.FsType, 0, ""); err != nil {
			if !os.IsNotExist(err) {
				return err
			}

			d.dmtool.DeleteDevice(devname)
		}
	}

	return nil
}

func (d *overlitDriver) Create(id, parent, mountLabel string, storageOpt map[string]string) (rerr error) {
	log.Printf("overlit: create (id = %s, parent = %s, mountLabel = %s, storageOpt = %v)\n", id, parent, mountLabel, storageOpt)

	dir := d.getHomePath(id)

	root, _, _, err := d.getRootIdentity()
	if err != nil {
		return err
	}

	if err := d.createHomeDir(id, parent, root); err != nil {
		return err
	}
	defer func() {
		if rerr != nil {
			os.RemoveAll(dir)
		}
	}()

	if err := d.createSubDir(id, parent, root); err != nil {
		return err
	}

	if err := d.dmtool.CreateDevice(id); err != nil {
		return err
	}

	return nil
}

func (d *overlitDriver) CreateReadWrite(id, parent, mountLabel string, storageOpt map[string]string) (rerr error) {
	log.Printf("overlit: createreadwrite (id = %s, parent = %s, mountLabel = %s, storageOpt = %v)\n", id, parent, mountLabel, storageOpt)

	dir := d.getHomePath(id)

	root, _, _, err := d.getRootIdentity()
	if err != nil {
		return err
	}

	if err := d.createHomeDir(id, parent, root); err != nil {
		return err
	}
	defer func() {
		if rerr != nil {
			os.RemoveAll(dir)
		}
	}()

	fstype, fssize, err := parseRWFSOptions(storageOpt)
	if err != nil {
		return err
	} else if fstype != "" {
		devPath := d.getDevPath(id)

		if err := d.dmtool.CreateDevice(id); err != nil {
			return errors.New("could not create device")
		}
		defer func() {
			if rerr != nil {
				d.dmtool.DeleteDevice(id)
			}
		}()

		if err := d.dmtool.ResizeDevice(id, fssize); err != nil {
			return errors.New("could not resize device")
		}

		if err := d.execCommands(fmt.Sprintf("mkfs.%v,%v", fstype, devPath)); err != nil {
			return err
		}

		if err := unix.Mount(devPath, dir, fstype, 0, ""); err != nil {
			return err
		}

		if err := d.dmtool.SetDeviceFsType(id, fstype); err != nil {
			return err
		}

		if err := d.dmtool.SetDeviceMntPath(id, dir); err != nil {
			return err
		}

		if err := d.dmtool.SetDeviceReadonly(id, false); err != nil {
			return err
		}

		if err := d.dmtool.Flush(); err != nil {
			return err
		}
	}

	if err := d.createSubDir(id, parent, root); err != nil {
		return err
	}

	return nil
}

func (d *overlitDriver) Remove(id string) error {
	log.Printf("overlit: remove (id = %s)\n", id)

	d.locker.Lock(id)
	defer d.locker.Unlock(id)

	dir := d.getHomePath(id)

	lid, err := ioutil.ReadFile(d.getLinkPath(dir))
	if err == nil {
		if err := os.RemoveAll(path.Join(d.home, linkDir, string(lid))); err != nil {
			log.Printf("overlit: failed to remove link: %v", err)
		}
	}

	// Unmount and delete the device if this layer has a logical volume device
	if mntpath, err := d.dmtool.GetDeviceMntPath(id); err == nil {
		mount.RecursiveUnmount(mntpath)
		d.dmtool.DeleteDevice(id)
	}

	if err := system.EnsureRemoveAll(dir); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func (d *overlitDriver) Get(id, mountLabel string) (_ containerfs.ContainerFS, rerr error) {
	log.Printf("overlit: get (id = %s)\n", id)

	d.locker.Lock(id)
	defer d.locker.Unlock(id)

	dir := d.getHomePath(id)

	if readonly, err := d.dmtool.GetDeviceReadonly(id); err == nil {
		if readonly == true {
			return containerfs.NewLocalContainerFS(d.getDiffPath(id)), nil
		}
	}

	lower, err := ioutil.ReadFile(d.getLowerPath(dir))
	if err != nil {
		if os.IsNotExist(err) {
			return containerfs.NewLocalContainerFS(d.getDiffPath(id)), nil
		}
		return nil, err
	}

	mergedPath := d.getMergedPath(dir)
	if count := d.ctr.Increment(mergedPath); count > 1 {
		return containerfs.NewLocalContainerFS(mergedPath), nil
	}
	defer func() {
		if rerr != nil {
			if c := d.ctr.Decrement(mergedPath); c <= 0 {
				if mntErr := unix.Unmount(mergedPath, 0); mntErr != nil {
					log.Printf("overlit: failed to mount %v: %v", mergedPath, mntErr)
				}
				if rmErr := unix.Rmdir(mergedPath); rmErr != nil && !os.IsNotExist(rmErr) {
					log.Printf("overlit: failed to remove %s: %v, %v", id, rmErr, err)
				}
			}
		}
	}()

	lowers := strings.Split(string(lower), ":")
	opts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", strings.Join(getAbsPaths(d.home, lowers), ":"), d.getDiffPath(dir), d.getWorkPath(dir))
	mountData := label.FormatMountLabel(opts, mountLabel)
	mount := unix.Mount
	mountTarget := mergedPath

	root, rootUID, rootGID, err := d.getRootIdentity()
	if err != nil {
		return nil, err
	}
	if err := idtools.MkdirAndChown(mergedPath, 0700, root); err != nil {
		return nil, err
	}

	if len(mountData) > pageSize {
		opts = fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", string(lower), d.getDiffPath(dir), d.getWorkPath(dir))
		mountData = label.FormatMountLabel(opts, mountLabel)
		if len(mountData) > pageSize {
			return nil, errors.Errorf("could not mount layer, mount label too large %d", len(mountData))
		}

		mount = func(source string, target string, mType string, flags uintptr, label string) error {
			return mountFrom(d.home, source, target, mType, flags, label)
		}
		mountTarget = d.getMergedPath(dir)
	}

	if err := mount("overlay", mountTarget, "overlay", 0, mountData); err != nil {
		return nil, errors.Errorf("error creating overlay mount to %s: %v", mergedPath, err)
	}

	if err := os.Chown(d.getWorkPath(dir), rootUID, rootGID); err != nil {
		return nil, err
	}

	return containerfs.NewLocalContainerFS(mergedPath), nil
}

func (d *overlitDriver) Put(id string) error {
	log.Printf("overlit: put (id = %s)\n", id)

	d.locker.Lock(id)
	defer d.locker.Unlock(id)

	dir := d.getHomePath(id)

	if readonly, err := d.dmtool.GetDeviceReadonly(id); err == nil {
		if readonly == true {
			return nil
		}
	}

	_, err := ioutil.ReadFile(d.getLowerPath(dir))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	mountpoint := d.getMergedPath(dir)
	if count := d.ctr.Decrement(mountpoint); count > 0 {
		return nil
	}
	if err := unix.Unmount(mountpoint, unix.MNT_DETACH); err != nil {
		log.Printf("overlit: failed to unmount %s: %s, %v", id, mountpoint, err)
	}
	if err := unix.Rmdir(mountpoint); err != nil && !os.IsNotExist(err) {
		log.Printf("overlit: failed to remove %s: %v", id, err)
	}

	return nil
}

func (d *overlitDriver) Exists(id string) bool {
	log.Printf("overlit: exists (id = %s)\n", id)

	_, err := os.Stat(d.getHomePath(id))

	return err == nil
}

func (d *overlitDriver) Status() [][2]string {
	log.Printf("overlit: status\n")

	return nil
}

func (d *overlitDriver) GetMetadata(id string) (map[string]string, error) {
	log.Printf("overlit: getmetadata (id = %s)\n", id)

	dir := d.getHomePath(id)

	metadata := map[string]string{
		"WorkDir":   d.getWorkPath(dir),
		"MergedDir": d.getMergedPath(dir),
		"UpperDir":  d.getDiffPath(dir),
	}

	var lowers []string

	lower, err := ioutil.ReadFile(d.getLowerPath(dir))
	if err == nil {
		for _, s := range strings.Split(string(lower), ":") {
			lp, err := os.Readlink(path.Join(d.home, s))
			if err != nil {
				return nil, err
			}
			lowers = append(lowers, path.Clean(path.Join(d.home, linkDir, lp)))
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	if len(lowers) > 0 {
		metadata["LowerDir"] = strings.Join(lowers, ":")
	}

	return metadata, nil
}

func (d *overlitDriver) Cleanup() error {
	log.Printf("overlit: cleanup\n")

	d.dmtool.Cleanup()

	if d.home != "" {
		return mount.RecursiveUnmount(d.home)
	}

	return nil
}

func (d *overlitDriver) Diff(id, parent string) io.ReadCloser {
	log.Printf("overlit: diff (id = %s, parent = %s)\n", id, parent)

	dir := d.getHomePath(id)
	diffPath := d.getDiffPath(dir)

	diff, _ := archive.TarWithOptions(diffPath, &archive.TarOptions{
		Compression:    archive.Uncompressed,
		UIDMaps:        d.uidMaps,
		GIDMaps:        d.gidMaps,
		WhiteoutFormat: archive.OverlayWhiteoutFormat,
	})

	return diff
}

func (d *overlitDriver) Changes(id, parent string) ([]gdhelper.Change, error) {
	log.Printf("overlit: changes (id = %s, parent = %s)\n", id, parent)

	dir := d.getHomePath(id)

	diffPath := d.getDiffPath(dir)
	parentPath := ""

	if parent != "" {
		pdir := d.getHomePath(parent)

		parentPath = d.getDiffPath(pdir)
	}

	changes, err := archive.ChangesDirs(diffPath, parentPath)
	if err != nil {
		return nil, err
	}

	return getGDHelperChanges(changes)
}

func (d *overlitDriver) ApplyDiff(id, parent string, diff io.Reader) (int64, error) {
	log.Printf("overlit: applydiff (id = %s, parent = %s)\n", id, parent)

	dir := d.getHomePath(id)
	tarsPath := d.getTarsPath(dir)
	diffPath := d.getDiffPath(dir)
	devPath := d.getDevPath(id)

	options := &archive.TarOptions{
		UIDMaps:        d.uidMaps,
		GIDMaps:        d.gidMaps,
		WhiteoutFormat: archive.OverlayWhiteoutFormat,
		InUserNS:       rsystem.RunningInUserNS(),
	}

	size, err := archive.ApplyUncompressedLayer(tarsPath, diff, options)
	if err != nil {
		return 0, err
	}

	if err := d.dmtool.ResizeDevice(id, uint64(math.Ceil(float64(size)*d.options.RofsRate))); err != nil {
		return 0, err
	}

	cmd0 := d.options.RofsCmd0
	cmd0 = strings.Replace(cmd0, "{tars}", tarsPath, -1)
	cmd0 = strings.Replace(cmd0, "{diff}", diffPath, -1)
	cmd0 = strings.Replace(cmd0, "{dev}", devPath, -1)
	if err := d.execCommands(cmd0); err != nil {
		return 0, err
	}

	if err := unix.Mount(devPath, diffPath, d.options.RofsType, 0, ""); err != nil {
		return 0, err
	}

	if err := d.dmtool.SetDeviceFsType(id, d.options.RofsType); err != nil {
		return 0, err
	}

	if err := d.dmtool.SetDeviceMntPath(id, d.getDiffPath(dir)); err != nil {
		return 0, err
	}

	if err := d.dmtool.SetDeviceReadonly(id, true); err != nil {
		return 0, err
	}

	if err := d.dmtool.Flush(); err != nil {
		return 0, err
	}

	cmd1 := d.options.RofsCmd1
	cmd1 = strings.Replace(cmd1, "{tars}", tarsPath, -1)
	cmd1 = strings.Replace(cmd1, "{diff}", diffPath, -1)
	cmd1 = strings.Replace(cmd1, "{dev}", devPath, -1)
	if err := d.execCommands(cmd1); err != nil {
		return 0, err
	}

	log.Printf("overlit: applydiff (size = %v)\n", size)

	return size, nil
}

func (d *overlitDriver) DiffSize(id, parent string) (int64, error) {
	log.Printf("overlit: diffsize (id = %s, parent = %s)\n", id, parent)

	return directory.Size(context.TODO(), d.getDiffPath(id))
}

func (d *overlitDriver) Capabilities() graphdriver.Capabilities {
	log.Printf("overlit: capabilities\n")

	return graphdriver.Capabilities{}
}

func NewOverlitDriver(options []string) (*overlitDriver, error) {
	log.Printf("overlit: createDriver ()\n")

	opts, err := parseOptions(options)
	if err != nil {
		return nil, err
	}

	d := &overlitDriver{}
	d.options = *opts
	d.dmtool = NewDmTool()

	// Check if overlayfs is available
	if err := checkFSAvailable("overlay"); err != nil {
		return nil, err
	}

	// Check if read-only filesystem is available
	if err := checkFSAvailable(d.options.RofsType); err != nil {
		return nil, err
	}

	return d, nil
}
