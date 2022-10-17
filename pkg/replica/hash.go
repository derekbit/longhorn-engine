package replica

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"

	"github.com/longhorn/backupstore"

	diskutil "github.com/longhorn/longhorn-engine/pkg/util/disk"
)

const (
	xattrSnapshotHashName     = "user.longhorn.hash"
	xattrSnapshotHashValueMax = 256
)

type SnapshotHashStatus struct {
	State    ProgressState
	Progress int
	Checksum string
	Error    string
}

type SnapshotHashTask struct {
	sync.Mutex

	SnapshotNames []string
	Rehash        bool

	replica *Replica

	SnapshotHashStatus
}

type SnapshotXattrHashInfo struct {
	Checksum string `json:"checksum"`
	ModTime  string `json:"modTime"`
}

func NewSnapshotHashTask(snapshotNames []string, rehash bool) *SnapshotHashTask {
	return &SnapshotHashTask{
		SnapshotNames: snapshotNames,
		Rehash:        rehash,

		SnapshotHashStatus: SnapshotHashStatus{
			State: ProgressStateInProgress,
		},
	}
}

func (t *SnapshotHashTask) Execute() {
	var err error
	var checksum string

	defer func() {
		t.Checksum = checksum
		if err != nil {
			t.State = ProgressStateError
			logrus.Errorf("failed to hash snapshot %v since %v", t.SnapshotNames, err)
		} else {
			t.State = ProgressStateComplete
		}
	}()

	fileInfo, err := t.statSnapshot()
	if err != nil {
		return
	}

	modTime := fileInfo.ModTime().String()

	requireRehash := true
	if !t.Rehash {
		requireRehash = t.isRehashRequired(modTime)
		if !requireRehash {
			return
		}
	}

	logrus.Infof("Starting hashing snapshot %v", t.SnapshotNames)

	checksum, err = t.hashSnapshot()
	if err != nil {
		return
	}

	logrus.Infof("Snapshot %v checksum %v", t.SnapshotNames, checksum)

	if err := t.setSnapshotHashInfoToXattr(checksum, modTime); err != nil {
		return
	}
}

func (t *SnapshotHashTask) openSnapshot() error {
	t.Lock()
	defer t.Unlock()

	dir, err := os.Getwd()
	if err != nil {
		return errors.Wrap(err, "cannot get working directory")
	}

	snapshotDiskNames := []string{}
	for _, name := range t.SnapshotNames {
		snapshotDiskNames = append(snapshotDiskNames, diskutil.GenerateSnapshotDiskName(name))
	}

	r, err := NewReadOnly(dir, snapshotDiskNames[0], snapshotDiskNames, nil)
	if err != nil {
		return err
	}

	t.replica = r

	return nil
}

func (t *SnapshotHashTask) closeSnapshot() error {
	t.Lock()
	defer t.Unlock()

	err := t.replica.Close()
	t.replica = nil

	return err
}

func (t *SnapshotHashTask) readSnapshot(start int64, data []byte) error {
	_, err := t.replica.ReadAt(data, start)
	return err
}

func (t *SnapshotHashTask) statSnapshot() (os.FileInfo, error) {
	return os.Stat(diskutil.GenerateSnapshotDiskName(t.SnapshotNames[0]))
}

func (t *SnapshotHashTask) getSnapshotHashInfoFromXattr() (string, string, error) {
	xattrSnapshotHashValue := make([]byte, xattrSnapshotHashValueMax)
	_, err := unix.Getxattr(diskutil.GenerateSnapshotDiskName(t.SnapshotNames[0]), xattrSnapshotHashName, xattrSnapshotHashValue)
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

func (t *SnapshotHashTask) setSnapshotHashInfoToXattr(checksum, modTime string) error {
	xattrSnapshotHashValue, err := json.Marshal(&SnapshotXattrHashInfo{
		Checksum: checksum,
		ModTime:  modTime,
	})
	if err != nil {
		return err
	}

	return unix.Setxattr(diskutil.GenerateSnapshotDiskName(t.SnapshotNames[0]), xattrSnapshotHashName, xattrSnapshotHashValue, 0)
}

func (t *SnapshotHashTask) getSize() int64 {
	return t.replica.info.Size
}

func (t *SnapshotHashTask) isRehashRequired(currentModTime string) bool {
	checksum, modTime, err := t.getSnapshotHashInfoFromXattr()
	if err != nil {
		logrus.Errorf("failed to get snapshot %v last hash info from xattr since %v", t.SnapshotNames[0], err)
		return true
	}

	if modTime != currentModTime || checksum == "" {
		return true
	}

	return false
}

func (t *SnapshotHashTask) hashSnapshot() (string, error) {
	err := t.openSnapshot()
	if err != nil {
		logrus.Errorf("failed to open snapshot %v since %v", t.SnapshotNames, err)
		return "", err
	}
	defer t.closeSnapshot()

	h := sha256.New()

	size := t.getSize()
	blkCounts := size / backupstore.DEFAULT_BLOCK_SIZE
	blkBuff := make([]byte, backupstore.DEFAULT_BLOCK_SIZE)

	for i := int64(0); i < blkCounts; i++ {
		offset := i * backupstore.DEFAULT_BLOCK_SIZE
		if err := t.readSnapshot(offset, blkBuff); err != nil {
			return "", err
		}
		written, err := h.Write(blkBuff)
		if err != nil {
			return "", err
		}

		t.Progress = int(100 * float64(offset+int64(written)) / float64(size))
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}
