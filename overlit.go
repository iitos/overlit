package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
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
}

type overlitDriver struct {
	options overlitOptions

	home string

	uidMaps []idtools.IDMap
	gidMaps []idtools.IDMap

	ctr    *graphdriver.RefCounter
	locker *locker.Locker
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
		default:
			return nil, fmt.Errorf("OVERLIT: Unknown option (%s = %s)", key, val)
		}
	}

	return opts, nil
}

func supportsOverlay() error {
	exec.Command("modprobe", "overlay").Run()

	f, err := os.Open("/proc/filesystems")
	if err != nil {
		return err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		if s.Text() == "nodev\toverlay" {
			return nil
		}
	}
	return graphdriver.ErrNotSupported
}

func (d *overlitDriver) dir(id string) string {
	return path.Join(d.home, id)
}

func (d *overlitDriver) getDiffPath(id string) string {
	dir := d.dir(id)

	return path.Join(dir, "diff")
}

func (d *overlitDriver) getLower(parent string) (string, error) {
	parentDir := d.dir(parent)

	if _, err := os.Lstat(parentDir); err != nil {
		return "", err
	}

	parentLink, err := ioutil.ReadFile(path.Join(parentDir, "link"))
	if err != nil {
		return "", err
	}
	lowers := []string{path.Join(linkDir, string(parentLink))}

	parentLower, err := ioutil.ReadFile(path.Join(parentDir, lowerFile))
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

	lowers, err := ioutil.ReadFile(path.Join(d.dir(id), lowerFile))
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

func (d *overlitDriver) isParent(id, parent string) bool {
	lowers, err := d.getLowerDirs(id)
	if err != nil {
		return false
	}
	if parent == "" && len(lowers) > 0 {
		return false
	}

	var ld string

	parentDir := d.dir(parent)
	if len(lowers) > 0 {
		ld = filepath.Dir(lowers[0])
	}
	if ld == "" && parent == "" {
		return true
	}
	return ld == parentDir
}

func (d *overlitDriver) create(id, parent string) (retErr error) {
	dir := d.dir(id)

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

	if err := ioutil.WriteFile(path.Join(dir, "link"), []byte(lid), 0644); err != nil {
		return err
	}

	if parent == "" {
		return nil
	}

	if err := idtools.MkdirAndChown(path.Join(dir, "work"), 0700, root); err != nil {
		return err
	}

	lower, err := d.getLower(parent)
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
	log.Printf("OVERLIT: Init (home = %s)\n", home)

	opts, err := parseOptions(options)
	if err != nil {
		return err
	}

	d.home = home
	d.uidMaps = uidMaps
	d.gidMaps = gidMaps
	d.ctr = graphdriver.NewRefCounter(graphdriver.NewFsChecker(graphdriver.FsMagicOverlay))
	d.locker = locker.New()
	d.options = *opts

	rootUID, rootGID, err := idtools.GetRootUIDGID(uidMaps, gidMaps)
	if err != nil {
		return err
	}
	if err := idtools.MkdirAllAndChown(path.Join(home, linkDir), 0700, idtools.Identity{UID: rootUID, GID: rootGID}); err != nil {
		return err
	}

	return nil
}

func (d *overlitDriver) Create(id, parent, mountLabel string, storageOpt map[string]string) error {
	log.Printf("OVERLIT: Create (id = %s, parent = %s, mountLabel = %s)\n", id, parent, mountLabel)

	return d.create(id, parent)
}

func (d *overlitDriver) CreateReadWrite(id, parent, mountLabel string, storageOpt map[string]string) error {
	log.Printf("OVERLIT: CreateReadWrite (id = %s, parent = %s, mountLabel = %s)\n", id, parent, mountLabel)

	for key, val := range storageOpt {
		switch key {
		default:
			return fmt.Errorf("OVERLIT: not supported option (%s = %s)", key, val)
		}
	}

	return d.create(id, parent)
}

func (d *overlitDriver) Remove(id string) error {
	log.Printf("OVERLIT: Remove (id = %s)\n", id)

	d.locker.Lock(id)
	defer d.locker.Unlock(id)
	dir := d.dir(id)
	lid, err := ioutil.ReadFile(path.Join(dir, "link"))
	if err == nil {
		if err := os.RemoveAll(path.Join(d.home, linkDir, string(lid))); err != nil {
			log.Printf("OVERLIT: Failed to remove link: %v", err)
		}
	}

	if err := system.EnsureRemoveAll(dir); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (d *overlitDriver) Get(id, mountLabel string) (_ containerfs.ContainerFS, retErr error) {
	log.Printf("OVERLIT: Get (id = %s)\n", id)

	d.locker.Lock(id)
	defer d.locker.Unlock(id)

	dir := d.dir(id)
	if _, err := os.Stat(dir); err != nil {
		return nil, err
	}

	diffDir := path.Join(dir, "diff")
	lowers, err := ioutil.ReadFile(path.Join(dir, lowerFile))
	if err != nil {
		if os.IsNotExist(err) {
			return containerfs.NewLocalContainerFS(diffDir), nil
		}
		return nil, err
	}

	mergedDir := path.Join(dir, "merged")
	if count := d.ctr.Increment(mergedDir); count > 1 {
		return containerfs.NewLocalContainerFS(mergedDir), nil
	}
	defer func() {
		if retErr != nil {
			if c := d.ctr.Decrement(mergedDir); c <= 0 {
				if mntErr := unix.Unmount(mergedDir, 0); mntErr != nil {
					log.Printf("OVERLIT: Failed to mount %v: %v", mergedDir, mntErr)
				}
				if rmErr := unix.Rmdir(mergedDir); rmErr != nil && !os.IsNotExist(rmErr) {
					log.Printf("OVERLIT: Failed to remove %s: %v, %v", id, rmErr, err)
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
	mountTarget := mergedDir

	rootUID, rootGID, err := idtools.GetRootUIDGID(d.uidMaps, d.gidMaps)
	if err != nil {
		return nil, err
	}
	if err := idtools.MkdirAndChown(mergedDir, 0700, idtools.Identity{UID: rootUID, GID: rootGID}); err != nil {
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
			return nil, fmt.Errorf("OVERLIT: cannot mount layer, mount label too large %d", len(mountData))
		}

		mount = func(source string, target string, mType string, flags uintptr, label string) error {
			return mountFrom(d.home, source, target, mType, flags, label)
		}
		mountTarget = path.Join(id, "merged")
	}

	if err := mount("overlay", mountTarget, "overlay", 0, mountData); err != nil {
		return nil, fmt.Errorf("error creating overlay mount to %s: %v", mergedDir, err)
	}

	if err := os.Chown(path.Join(workDir, "work"), rootUID, rootGID); err != nil {
		return nil, err
	}

	return containerfs.NewLocalContainerFS(mergedDir), nil
}

func (d *overlitDriver) Put(id string) error {
	log.Printf("OVERLIT: Put (id = %s)\n", id)

	d.locker.Lock(id)
	defer d.locker.Unlock(id)

	dir := d.dir(id)
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
		log.Printf("OVERLIT: Failed to unmount %s: %s, %v", id, mountpoint, err)
	}
	if err := unix.Rmdir(mountpoint); err != nil && !os.IsNotExist(err) {
		log.Printf("OVERLIT: Failed to remove %s: %v", id, err)
	}

	return nil
}

func (d *overlitDriver) Exists(id string) bool {
	log.Printf("OVERLIT: Exists (id = %s)\n", id)

	_, err := os.Stat(d.dir(id))

	return err == nil
}

func (d *overlitDriver) Status() [][2]string {
	log.Printf("OVERLIT: Status\n")

	return nil
}

func (d *overlitDriver) GetMetadata(id string) (map[string]string, error) {
	log.Printf("OVERLIT: GetMetadata (id = %s)\n", id)

	dir := d.dir(id)
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
	log.Printf("OVERLIT: Cleanup\n")

	return mount.RecursiveUnmount(d.home)
}

func (d *overlitDriver) Diff(id, parent string) io.ReadCloser {
	log.Printf("OVERLIT: Diff (id = %s, parent = %s)\n", id, parent)

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
	log.Printf("OVERLIT: Changes (id = %s, parent = %s)\n", id, parent)

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
		changes[i] = graphhelper.Change{Path: _change.Path, Kind: graphhelper.ChangeKind(_change.Kind)}
	}

	return changes, nil
}

func (d *overlitDriver) ApplyDiff(id, parent string, diff io.Reader) (int64, error) {
	log.Printf("OVERLIT: ApplyDiff (id = %s, parent = %s)\n", id, parent)

	applyDir := d.getDiffPath(id)

	options := &archive.TarOptions{
		UIDMaps:        d.uidMaps,
		GIDMaps:        d.gidMaps,
		WhiteoutFormat: archive.OverlayWhiteoutFormat,
		InUserNS:       rsystem.RunningInUserNS(),
	}

	size, err := archive.ApplyUncompressedLayer(applyDir, diff, options)
	if err != nil {
		return 0, err
	}

	return size, nil
}

func (d *overlitDriver) DiffSize(id, parent string) (int64, error) {
	log.Printf("OVERLIT: DiffSize (id = %s, parent = %s)\n", id, parent)

	return directory.Size(context.TODO(), d.getDiffPath(id))
}

func (d *overlitDriver) Capabilities() graphdriver.Capabilities {
	log.Printf("OVERLIT: Capabilities\n")

	return graphdriver.Capabilities{}
}

func newOverlitDriver() (*overlitDriver, error) {
	log.Printf("OVERLIT: CreateDriver ()\n")

	d := &overlitDriver{}

	return d, nil
}