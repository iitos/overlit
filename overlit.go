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

	graphhelper "github.com/docker/go-plugins-helpers/graphdriver"
	rsystem "github.com/opencontainers/runc/libcontainer/system"

	"github.com/opencontainers/selinux/go-selinux/label"
	"golang.org/x/sys/unix"
)

const (
	driverName = "overlit"
	linkDir    = "l"
	lowerFile  = "lower"
	maxDepth   = 128
	idLength   = 26
)

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

func (d *overlitDriver) getHomePath(id string) string {
	return path.Join(d.home, id)
}

func (d *overlitDriver) getDiffPath(id string) string {
	dir := d.getHomePath(id)

	return path.Join(dir, "diff")
}

func (d *overlitDriver) getTarsPath(id string) string {
	dir := d.getHomePath(id)

	return path.Join(dir, "tars")
}

func (d *overlitDriver) getDevPath(id string) string {
	return path.Join("/dev/mapper", id)
}

func (d *overlitDriver) getLowerPath(parent string) (string, error) {
	parentPath := d.getHomePath(parent)

	if _, err := os.Lstat(parentPath); err != nil {
		return "", err
	}

	parentLink, err := ioutil.ReadFile(path.Join(parentPath, "link"))
	if err != nil {
		return "", err
	}
	lowers := []string{path.Join(linkDir, string(parentLink))}

	parentLower, err := ioutil.ReadFile(path.Join(parentPath, lowerFile))
	if err == nil {
		parentLowers := strings.Split(string(parentLower), ":")
		lowers = append(lowers, parentLowers...)
	}
	if len(lowers) > maxDepth {
		return "", errors.New("max depth exceeded")
	}
	return strings.Join(lowers, ":"), nil
}

func (d *overlitDriver) getLowerDirs(id string) ([]string, error) {
	var lowersArray []string

	lowers, err := ioutil.ReadFile(path.Join(d.getHomePath(id), lowerFile))
	if err == nil {
		for _, s := range strings.Split(string(lowers), ":") {
			lp, err := os.Readlink(path.Join(d.home, s))
			if err != nil {
				return nil, err
			}
			lowersArray = append(lowersArray, path.Clean(path.Join(d.home, linkDir, lp)))
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return lowersArray, nil
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

func (d *overlitDriver) create(id, parent string) (retErr error) {
	dir := d.getHomePath(id)

	rootUID, rootGID, err := idtools.GetRootUIDGID(d.uidMaps, d.gidMaps)
	if err != nil {
		return err
	}
	root := idtools.Identity{UID: rootUID, GID: rootGID}

	if err := idtools.MkdirAllAndChown(path.Dir(dir), 0700, root); err != nil {
		return err
	}
	if err := idtools.MkdirAndChown(dir, 0700, root); err != nil {
		return err
	}

	lid := generateID(idLength)

	defer func() {
		if retErr != nil {
			os.RemoveAll(dir)
		}
	}()

	if err := idtools.MkdirAndChown(path.Join(dir, "diff"), 0755, root); err != nil {
		return err
	}

	if err := os.Symlink(path.Join("..", id, "diff"), path.Join(d.home, linkDir, lid)); err != nil {
		return err
	}

	if err := idtools.MkdirAndChown(path.Join(dir, "tars"), 0755, root); err != nil {
		return err
	}

	if err := ioutil.WriteFile(path.Join(dir, "link"), []byte(lid), 0644); err != nil {
		return err
	}

	if parent == "" {
		return nil
	}

	if err := idtools.MkdirAndChown(path.Join(dir, "work"), 0700, root); err != nil {
		return err
	}

	lower, err := d.getLowerPath(parent)
	if err != nil {
		return err
	}

	if lower != "" {
		if err := ioutil.WriteFile(path.Join(dir, lowerFile), []byte(lower), 0666); err != nil {
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

	rootUID, rootGID, err := idtools.GetRootUIDGID(uidMaps, gidMaps)
	if err != nil {
		return err
	}
	if err := idtools.MkdirAllAndChown(path.Join(home, linkDir), 0700, idtools.Identity{UID: rootUID, GID: rootGID}); err != nil {
		return err
	}

	if err := d.dmtool.Setup(d.options.DevName, d.options.ExtentSize, fmt.Sprintf("%v/dmtool.json", d.home)); err != nil {
		return err
	}

	return nil
}

func (d *overlitDriver) Create(id, parent, mountLabel string, storageOpt map[string]string) error {
	log.Printf("overlit: create (id = %s, parent = %s, mountLabel = %s)\n", id, parent, mountLabel)

	return d.create(id, parent)
}

func (d *overlitDriver) CreateReadWrite(id, parent, mountLabel string, storageOpt map[string]string) error {
	log.Printf("overlit: createreadwrite (id = %s, parent = %s, mountLabel = %s)\n", id, parent, mountLabel)

	for key, val := range storageOpt {
		switch key {
		default:
			return errors.Errorf("not supported option (%s = %s)", key, val)
		}
	}

	return d.create(id, parent)
}

func (d *overlitDriver) Remove(id string) error {
	log.Printf("overlit: remove (id = %s)\n", id)

	d.locker.Lock(id)
	defer d.locker.Unlock(id)

	dir := d.getHomePath(id)

	lid, err := ioutil.ReadFile(path.Join(dir, "link"))
	if err == nil {
		if err := os.RemoveAll(path.Join(d.home, linkDir, string(lid))); err != nil {
			log.Printf("overlit: failed to remove link: %v", err)
		}
	}

	// Unmount and delete the device if this layer has a logical volume device
	devPath := d.getDevPath(id)
	if _, err := os.Stat(devPath); err == nil {
		unix.Unmount(devPath, unix.MNT_DETACH)
		d.dmtool.DeleteDevice(id)
	}

	if err := system.EnsureRemoveAll(dir); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func (d *overlitDriver) Get(id, mountLabel string) (_ containerfs.ContainerFS, retErr error) {
	log.Printf("overlit: get (id = %s)\n", id)

	d.locker.Lock(id)
	defer d.locker.Unlock(id)

	dir := d.getHomePath(id)
	if _, err := os.Stat(dir); err != nil {
		return nil, err
	}

	diffPath := path.Join(dir, "diff")
	lowers, err := ioutil.ReadFile(path.Join(dir, lowerFile))
	if err != nil {
		if os.IsNotExist(err) {
			return containerfs.NewLocalContainerFS(diffPath), nil
		}
		return nil, err
	}

	mergedPath := path.Join(dir, "merged")
	if count := d.ctr.Increment(mergedPath); count > 1 {
		return containerfs.NewLocalContainerFS(mergedPath), nil
	}
	defer func() {
		if retErr != nil {
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

	workDir := path.Join(dir, "work")
	splitLowers := strings.Split(string(lowers), ":")
	absLowers := make([]string, len(splitLowers))
	for i, s := range splitLowers {
		absLowers[i] = path.Join(d.home, s)
	}
	opts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", strings.Join(absLowers, ":"), path.Join(dir, "diff"), path.Join(dir, "work"))
	mountData := label.FormatMountLabel(opts, mountLabel)
	mount := unix.Mount
	mountTarget := mergedPath

	rootUID, rootGID, err := idtools.GetRootUIDGID(d.uidMaps, d.gidMaps)
	if err != nil {
		return nil, err
	}
	if err := idtools.MkdirAndChown(mergedPath, 0700, idtools.Identity{UID: rootUID, GID: rootGID}); err != nil {
		return nil, err
	}

	pageSize := unix.Getpagesize()

	if pageSize > 4096 {
		pageSize = 4096
	}

	if len(mountData) > pageSize {
		opts = fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", string(lowers), path.Join(id, "diff"), path.Join(id, "work"))
		mountData = label.FormatMountLabel(opts, mountLabel)
		if len(mountData) > pageSize {
			return nil, errors.Errorf("could not mount layer, mount label too large %d", len(mountData))
		}

		mount = func(source string, target string, mType string, flags uintptr, label string) error {
			return mountFrom(d.home, source, target, mType, flags, label)
		}
		mountTarget = path.Join(id, "merged")
	}

	if err := mount("overlay", mountTarget, "overlay", 0, mountData); err != nil {
		return nil, errors.Errorf("error creating overlay mount to %s: %v", mergedPath, err)
	}

	if err := os.Chown(path.Join(workDir, "work"), rootUID, rootGID); err != nil {
		return nil, err
	}

	return containerfs.NewLocalContainerFS(mergedPath), nil
}

func (d *overlitDriver) Put(id string) error {
	log.Printf("overlit: put (id = %s)\n", id)

	d.locker.Lock(id)
	defer d.locker.Unlock(id)

	dir := d.getHomePath(id)
	_, err := ioutil.ReadFile(path.Join(dir, lowerFile))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	mountpoint := path.Join(dir, "merged")
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
	if _, err := os.Stat(dir); err != nil {
		return nil, err
	}

	metadata := map[string]string{
		"WorkDir":   path.Join(dir, "work"),
		"MergedDir": path.Join(dir, "merged"),
		"UpperDir":  path.Join(dir, "diff"),
	}

	lowerDirs, err := d.getLowerDirs(id)
	if err != nil {
		return nil, err
	}
	if len(lowerDirs) > 0 {
		metadata["LowerDir"] = strings.Join(lowerDirs, ":")
	}

	return metadata, nil
}

func (d *overlitDriver) Cleanup() error {
	log.Printf("overlit: cleanup\n")

	d.dmtool.Cleanup()

	return mount.RecursiveUnmount(d.home)
}

func (d *overlitDriver) Diff(id, parent string) io.ReadCloser {
	log.Printf("overlit: diff (id = %s, parent = %s)\n", id, parent)

	diffPath := d.getDiffPath(id)

	diff, _ := archive.TarWithOptions(diffPath, &archive.TarOptions{
		Compression:    archive.Uncompressed,
		UIDMaps:        d.uidMaps,
		GIDMaps:        d.gidMaps,
		WhiteoutFormat: archive.OverlayWhiteoutFormat,
	})

	return diff
}

func (d *overlitDriver) Changes(id, parent string) ([]graphhelper.Change, error) {
	log.Printf("overlit: changes (id = %s, parent = %s)\n", id, parent)

	diffPath := d.getDiffPath(id)
	parentPath := ""

	if parent != "" {
		parentPath = d.getDiffPath(parent)
	}

	_changes, err := archive.ChangesDirs(diffPath, parentPath)
	if err != nil {
		return nil, err
	}

	changes := make([]graphhelper.Change, len(_changes))

	for i, _change := range _changes {
		changes[i] = graphhelper.Change{
			Path: _change.Path,
			Kind: graphhelper.ChangeKind(_change.Kind),
		}
	}

	return changes, nil
}

func (d *overlitDriver) ApplyDiff(id, parent string, diff io.Reader) (int64, error) {
	log.Printf("overlit: applydiff (id = %s, parent = %s)\n", id, parent)

	tarsPath := d.getTarsPath(id)
	diffPath := d.getDiffPath(id)
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

	if err := d.dmtool.CreateDevice(id, uint64(math.Ceil(float64(size)*d.options.RofsRate))); err != nil {
		return 0, err
	}

	cmd0 := d.options.RofsCmd0
	cmd0 = strings.Replace(cmd0, "{tars}", tarsPath, -1)
	cmd0 = strings.Replace(cmd0, "{diff}", diffPath, -1)
	cmd0 = strings.Replace(cmd0, "{dev}", devPath, -1)
	if err := d.execCommands(cmd0); err != nil {
		return 0, err
	}

	if err := unix.Mount(devPath, diffPath, d.options.RofsType, 0, id); err != nil {
		return 0, err
	}

	cmd1 := d.options.RofsCmd1
	cmd1 = strings.Replace(cmd1, "{tars}", tarsPath, -1)
	cmd1 = strings.Replace(cmd1, "{diff}", diffPath, -1)
	cmd1 = strings.Replace(cmd1, "{dev}", devPath, -1)
	if err := d.execCommands(cmd1); err != nil {
		return 0, err
	}

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
	if err := checkOverlayFSAvailable(); err != nil {
		return nil, err
	}

	return d, nil
}
