package replica

import (
	"bytes"
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

	fileLockDirectory = "/host/var/lib/longhorn/.lock"
	hashLockFileName  = "hash"
)

type SnapshotHashStatus struct {
	StatusLock sync.RWMutex

	State             ProgressState
	Checksum          string
	Error             string
	SilentlyCorrupted bool
}

type SnapshotHashTask struct {
	sync.Mutex

	SnapshotName string
	Rehash       bool

	file *os.File

	SnapshotHashStatus
}

type SnapshotXattrHashInfo struct {
	Method       string `json:"method"`
	Checksum     string `json:"checksum"`
	ModTime      string `json:"modTime"`
	LastHashedAt string `json:"lastHashedAt"`
}

func NewSnapshotHashTask(snapshotName string, rehash bool) *SnapshotHashTask {
	return &SnapshotHashTask{
		SnapshotName: snapshotName,
		Rehash:       rehash,

		SnapshotHashStatus: SnapshotHashStatus{
			State: ProgressStateInProgress,
		},
	}
}

func (t *SnapshotHashTask) LockFile() (fileLock *flock.Flock, err error) {
	defer func() {
		if err != nil && fileLock != nil && fileLock.Path() != "" {
			if err := os.RemoveAll(fileLock.Path()); err != nil {
				logrus.Warnf("failed to remove lock file %v since %v", fileLock.Path(), err)
			}
		}
	}()

	hostName := os.Getenv("HOSTNAME")
	if hostName == "" {
		return nil, fmt.Errorf("env HOSTNAME is missing")
	}

	err = os.MkdirAll(fileLockDirectory, 0755)
	if err != nil {
		return nil, err
	}

	fileLock = flock.New(filepath.Join(fileLockDirectory, hashLockFileName))

	// Blocking lock
	err = fileLock.Lock()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to fetch the file lock for hashing snapshot %v", t.SnapshotName)
	}

	return fileLock, nil
}

func (t *SnapshotHashTask) UnlockFile(fileLock *flock.Flock) {
	fileLock.Unlock()
}

func (t *SnapshotHashTask) Execute() {
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

	// Each instance-manager-r pod can have multiple replica processes and can hash the snapshot disk files.
	// When the snapshot hashing task is initiated, the task will try to fetch the file lock (${fileLockDirectory}/hash).
	// The file is unlocked and deleted after the task is completed.
	fileLock, err := t.LockFile()
	if err != nil {
		return
	}
	defer t.UnlockFile(fileLock)

	modTime, err := GetSnapshotModTime(t.SnapshotName)
	if err != nil {
		return
	}

	requireRehash := true
	if !t.Rehash {
		requireRehash, checksum = t.isRehashRequired(modTime)
		if !requireRehash {
			return
		}
	}

	logrus.Infof("Starting hashing snapshot %v", t.SnapshotName)

	lastHashedAt := time.Now().UTC().Format(time.RFC3339)
	checksum, err = hashSnapshot(t.SnapshotName)
	if err != nil {
		return
	}

	if t.isSnapshotSilentlyCorrupted(checksum) {
		t.StatusLock.Lock()
		t.SilentlyCorrupted = true
		t.StatusLock.Unlock()
		return
	}

	logrus.Infof("Snapshot %v checksum %v", t.SnapshotName, checksum)

	err = SetSnapshotHashInfoToXattr(t.SnapshotName, checksum, modTime, lastHashedAt)
	if err != nil {
		return
	}

	err = t.isModTimeRemain(modTime)
	if err != nil {
		err = DeleteSnapshotHashInfoFromXattr(t.SnapshotName)
	}
}

func (t *SnapshotHashTask) isSnapshotSilentlyCorrupted(checksum string) bool {
	// To detect the silent corruption, read the modTime and checksum already recorded in the snapshot disk file first.
	// Then, rehash the file and compare the modTimes and checksums.
	// If the modTimes are identical but the checksums differ, the file is silently corrupted.

	existingChecksum := ""
	existingModTime := ""

	existingChecksum, existingModTime, err := GetSnapshotHashInfoFromXattr(t.SnapshotName)
	if err != nil && err != syscall.ENODATA {
		return false
	}

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

func GetSnapshotHashInfoFromXattr(snapshotName string) (string, string, error) {
	xattrSnapshotHashValue := make([]byte, xattrSnapshotHashValueMax)
	_, err := unix.Getxattr(diskutil.GenerateSnapshotDiskName(snapshotName), xattrSnapshotHashName, xattrSnapshotHashValue)
	if err != nil {
		return "", "", err
	}

	index := bytes.IndexByte(xattrSnapshotHashValue, 0)

	info := &SnapshotXattrHashInfo{}
	if err := json.Unmarshal(xattrSnapshotHashValue[:index], info); err != nil {
		return "", "", err
	}

	return info.Checksum, info.ModTime, nil
}

func SetSnapshotHashInfoToXattr(snapshotName, checksum, modTime, lastHashedAt string) error {
	xattrSnapshotHashValue, err := json.Marshal(&SnapshotXattrHashInfo{
		Method:       defaultHashMethod,
		Checksum:     checksum,
		ModTime:      modTime,
		LastHashedAt: lastHashedAt,
	})
	if err != nil {
		return err
	}

	return unix.Setxattr(diskutil.GenerateSnapshotDiskName(snapshotName), xattrSnapshotHashName, xattrSnapshotHashValue, 0)
}

func DeleteSnapshotHashInfoFromXattr(snapshotName string) error {
	return unix.Removexattr(diskutil.GenerateSnapshotDiskName(snapshotName), xattrSnapshotHashName)
}

func (t *SnapshotHashTask) getSize() (int64, error) {
	fileInfo, err := t.file.Stat()
	if err != nil {
		return -1, err
	}

	return fileInfo.Size(), nil
}

func (t *SnapshotHashTask) isRehashRequired(currentModTime string) (bool, string) {
	checksum, modTime, err := GetSnapshotHashInfoFromXattr(t.SnapshotName)
	if err != nil {
		logrus.Debugf("failed to get snapshot %v last hash info from xattr since %v", t.SnapshotName, err)
		return true, ""
	}

	if modTime != currentModTime || checksum == "" {
		return true, ""
	}

	return false, checksum
}

func (t *SnapshotHashTask) isModTimeRemain(oldModTime string) error {
	newModTime, err := GetSnapshotModTime(t.SnapshotName)
	if err != nil {
		return err
	}

	if oldModTime != newModTime {
		return fmt.Errorf("snapshot %v modification time is changed", t.SnapshotName)
	}

	return nil
}

func hashSnapshot(snapshotName string) (string, error) {
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

	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

func newHashMethod(method string) (hash.Hash, error) {
	switch method {
	case "crc64":
		return crc64.New(crc64.MakeTable(crc64.ISO)), nil
	default:
		return nil, fmt.Errorf("invalid hash method %v", method)
	}
}
