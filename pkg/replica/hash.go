package replica

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/gofrs/flock"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"

	diskutil "github.com/longhorn/longhorn-engine/pkg/util/disk"
)

const (
	defaultHashMethod = "crc64"

	xattrSnapshotHashName     = "user.longhorn.metadata"
	xattrSnapshotHashValueMax = 256

	FileLockDirectory = "/host/var/lib/longhorn/.lock"
	HashLockFileName  = "hash"
)

type SnapshotHashStatus struct {
	StatusLock sync.RWMutex

	State             ProgressState
	Checksum          string
	Error             string
	SilentlyCorrupted bool
}

type SnapshotHashJob struct {
	sync.Mutex

	Ctx        context.Context
	CancelFunc context.CancelFunc

	SnapshotName string
	Rehash       bool

	file *os.File

	SnapshotHashStatus
}

type SnapshotXattrHashInfo struct {
	Method            string `json:"method"`
	Checksum          string `json:"checksum"`
	ModTime           string `json:"modTime"`
	LastHashedAt      string `json:"lastHashedAt"`
	SilentlyCorrupted bool   `json:"silentlyCorrupted"`
}

func NewSnapshotHashJob(ctx context.Context, cancel context.CancelFunc, snapshotName string, rehash bool) *SnapshotHashJob {
	return &SnapshotHashJob{
		Ctx:          ctx,
		CancelFunc:   cancel,
		SnapshotName: snapshotName,
		Rehash:       rehash,

		SnapshotHashStatus: SnapshotHashStatus{
			State: ProgressStateInProgress,
		},
	}
}

func (t *SnapshotHashJob) LockFile() (fileLock *flock.Flock, err error) {
	defer func() {
		if err != nil && fileLock != nil && fileLock.Path() != "" {
			if err := os.RemoveAll(fileLock.Path()); err != nil {
				logrus.Warnf("failed to remove lock file %v since %v", fileLock.Path(), err)
			}
		}
	}()

	err = os.MkdirAll(FileLockDirectory, 0755)
	if err != nil {
		return nil, err
	}

	fileLock = flock.New(filepath.Join(FileLockDirectory, HashLockFileName))

	// Blocking lock
	err = fileLock.Lock()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to fetch the file lock for hashing snapshot %v", t.SnapshotName)
	}

	return fileLock, nil
}

func (t *SnapshotHashJob) UnlockFile(fileLock *flock.Flock) {
	fileLock.Unlock()
}

func (t *SnapshotHashJob) Execute() {
	var err error
	var checksum string

	defer func() {
		t.StatusLock.Lock()
		defer t.StatusLock.Unlock()
		t.Checksum = checksum
		if err != nil {
			logrus.Errorf("failed to hash snapshot %v since %v", t.SnapshotName, err)
			t.State = ProgressStateError
			t.Error = err.Error()
		} else {
			t.State = ProgressStateComplete
		}
	}()

	// Each node can have only one snapshot hashing task at the same time per node.
	// When the snapshot hashing task is started, the task tries to fetch the file lock
	// (${fileLockDirectory}/hash). If the lock file is held by another task, it will stuck
	// here and wait for the lock. The file is unlocked after the task is completed.
	fileLock, err := t.LockFile()
	if err != nil {
		return
	}
	defer t.UnlockFile(fileLock)

	modTime, err := GetSnapshotModTime(t.SnapshotName)
	if err != nil {
		return
	}

	// If the silent corruption is detected, don't need to recalculate the checksum.
	// Just set SilentlyCorrupted to ture and return it.
	isSilentlyCorrupted, err := t.isSilentCorruptionAlreadyDetected(modTime)
	if err != nil {
		return
	} else if isSilentlyCorrupted {
		t.StatusLock.Lock()
		t.SilentlyCorrupted = true
		t.StatusLock.Unlock()
		return
	}

	requireRehash := true
	if !t.Rehash {
		requireRehash, checksum, err = t.isRehashRequired(modTime)
		if err != nil {
			return
		}
		if !requireRehash {
			return
		}
	}

	logrus.Infof("Starting hashing snapshot %v", t.SnapshotName)

	lastHashedAt := time.Now().UTC().Format(time.RFC3339)
	checksum, err = hashSnapshot(t.Ctx, t.SnapshotName)
	if err != nil {
		return
	}

	// If the silent corruption is detected, the xattr will not be overrode.
	// The scene will be preserved.
	if t.isSnapshotSilentlyCorrupted(checksum) {
		t.StatusLock.Lock()
		t.SilentlyCorrupted = true
		t.StatusLock.Unlock()
		// Do the best to set SilentlyCorrupted to true.
		// Never mind if failed to set it. The checksum will be recalculated and get the SilentlyCorrupted again.
		if err := SetSilentlyCorruptedToXattr(t.SnapshotName); err != nil {
			logrus.Warnf("failed to set SilentlyCorrupted to true since %v", err.Error())
		}
		return
	}

	logrus.Infof("Snapshot %v checksum %v", t.SnapshotName, checksum)

	err = SetSnapshotHashInfoToXattr(t.SnapshotName, &SnapshotXattrHashInfo{
		Method:            defaultHashMethod,
		Checksum:          checksum,
		ModTime:           modTime,
		LastHashedAt:      lastHashedAt,
		SilentlyCorrupted: false,
	})
	if err != nil {
		return
	}

	err = t.isModTimeRemain(modTime)
	if err != nil {
		// Do the best to delete the useless xattr.
		// The deletion failure is acceptable, because the mismatching timestamps
		// will trigger the rehash in the next hash request.
		DeleteSnapshotHashInfoFromXattr(t.SnapshotName)
	}
}

func (t *SnapshotHashJob) isSnapshotSilentlyCorrupted(checksum string) bool {
	// To detect the silent corruption, read the modTime and checksum already recorded in the snapshot disk file first.
	// Then, rehash the file and compare the modTimes and checksums.
	// If the modTimes are identical but the checksums differ, the file is silently corrupted.

	info, err := GetSnapshotHashInfoFromXattr(t.SnapshotName)
	if err != nil || info == nil {
		return false
	}

	existingChecksum := info.Checksum
	existingModTime := info.ModTime

	if existingChecksum == "" || existingModTime == "" {
		return false
	}

	if err := t.isModTimeRemain(existingModTime); err != nil {
		return false
	}

	if checksum != existingChecksum {
		return true
	}

	return false
}

func GetSnapshotModTime(snapshotName string) (string, error) {
	fileInfo, err := os.Stat(diskutil.GenerateSnapshotDiskName(snapshotName))
	if err != nil {
		return "", err
	}

	return fileInfo.ModTime().String(), nil
}

func SetSilentlyCorruptedToXattr(snapshotName string) error {
	info, err := GetSnapshotHashInfoFromXattr(snapshotName)
	if err != nil && err != syscall.ENODATA {
		return err
	}

	if info == nil {
		info = &SnapshotXattrHashInfo{
			Method:       defaultHashMethod,
			Checksum:     info.Checksum,
			ModTime:      info.ModTime,
			LastHashedAt: info.LastHashedAt,
		}
	}

	info.SilentlyCorrupted = true

	return SetSnapshotHashInfoToXattr(snapshotName, info)
}

func GetSnapshotHashInfoFromXattr(snapshotName string) (*SnapshotXattrHashInfo, error) {
	xattrSnapshotHashValue := make([]byte, xattrSnapshotHashValueMax)
	_, err := unix.Getxattr(diskutil.GenerateSnapshotDiskName(snapshotName), xattrSnapshotHashName, xattrSnapshotHashValue)
	if err != nil {
		return nil, err
	}

	index := bytes.IndexByte(xattrSnapshotHashValue, 0)

	info := &SnapshotXattrHashInfo{}
	if err := json.Unmarshal(xattrSnapshotHashValue[:index], info); err != nil {
		return nil, err
	}

	return info, nil
}

func SetSnapshotHashInfoToXattr(snapshotName string, info *SnapshotXattrHashInfo) error {
	xattrSnapshotHashValue, err := json.Marshal(&SnapshotXattrHashInfo{
		Method:            defaultHashMethod,
		Checksum:          info.Checksum,
		ModTime:           info.ModTime,
		LastHashedAt:      info.LastHashedAt,
		SilentlyCorrupted: info.SilentlyCorrupted,
	})
	if err != nil {
		return err
	}

	return unix.Setxattr(diskutil.GenerateSnapshotDiskName(snapshotName), xattrSnapshotHashName, xattrSnapshotHashValue, 0)
}

func DeleteSnapshotHashInfoFromXattr(snapshotName string) error {
	return unix.Removexattr(diskutil.GenerateSnapshotDiskName(snapshotName), xattrSnapshotHashName)
}

func (t *SnapshotHashJob) isSilentCorruptionAlreadyDetected(currentModTime string) (bool, error) {
	info, err := GetSnapshotHashInfoFromXattr(t.SnapshotName)
	if err != nil || info == nil {
		if err != syscall.ENODATA {
			return false, errors.Wrapf(err, "failed to get snapshot %v last hash info from xattr", t.SnapshotName)
		}
		return false, nil
	}

	if currentModTime == info.ModTime {
		return info.SilentlyCorrupted, nil
	}

	return false, nil
}

func (t *SnapshotHashJob) isRehashRequired(currentModTime string) (bool, string, error) {
	info, err := GetSnapshotHashInfoFromXattr(t.SnapshotName)
	if err != nil || info == nil {
		if err != syscall.ENODATA {
			return true, "", errors.Wrapf(err, "failed to get snapshot %v last hash info from xattr", t.SnapshotName)
		}
		return true, "", nil
	}

	checksum := info.Checksum
	modTime := info.ModTime

	if modTime != currentModTime || checksum == "" {
		return true, "", nil
	}

	return false, checksum, nil
}

func (t *SnapshotHashJob) isModTimeRemain(oldModTime string) error {
	newModTime, err := GetSnapshotModTime(t.SnapshotName)
	if err != nil {
		return err
	}

	if oldModTime != newModTime {
		return fmt.Errorf("snapshot %v modification time is changed", t.SnapshotName)
	}

	return nil
}

func hashSnapshot(ctx context.Context, snapshotName string) (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", errors.Wrap(err, "cannot get working directory")
	}

	path := filepath.Join(dir, diskutil.GenerateSnapshotDiskName(snapshotName))

	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return "", errors.Wrapf(err, "failed to open %v", path)
	}
	defer f.Close()

	h, err := newHashMethod(defaultHashMethod)
	if err != nil {
		return "", err
	}

	if err := Copy(ctx, h, f); err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

type readerFunc func(p []byte) (n int, err error)

func (rf readerFunc) Read(p []byte) (n int, err error) {
	return rf(p)
}

func Copy(ctx context.Context, dst io.Writer, src io.Reader) error {
	_, err := io.Copy(dst, readerFunc(func(p []byte) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			return src.Read(p)
		}
	}))
	return err
}

func newHashMethod(method string) (hash.Hash, error) {
	switch method {
	case "crc64":
		return crc64.New(crc64.MakeTable(crc64.ISO)), nil
	default:
		return nil, fmt.Errorf("invalid hash method %v", method)
	}
}
