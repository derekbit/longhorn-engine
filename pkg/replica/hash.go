package replica

import (
	"bytes"
	"encoding/json"
	"os"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

const (
	xattrSnapshotHashName     = "user.longhorn.hash"
	xattrSnapshotHashValueMax = 256
)

type SnapshotHashStatus struct {
	lock sync.Mutex

	replica *Replica

	SnapshotNames []string

	State    ProgressState
	Progress int
	Checksum string
	Error    string
}

type SnapshotXattrHashInfo struct {
	Checksum string `json:"checksum"`
	ModTime  string `json:"modTime"`
}

func NewSnapshotHash(snapshotNames []string) *SnapshotHashStatus {
	return &SnapshotHashStatus{
		SnapshotNames: snapshotNames,
		State:         ProgressStateInProgress,
	}
}

func (s *SnapshotHashStatus) OpenSnapshot(snapshotNames []string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	dir, err := os.Getwd()
	if err != nil {
		return errors.Wrap(err, "cannot get working directory")
	}

	snapshotDiskNames := []string{}
	for _, name := range snapshotNames {
		snapshotDiskNames = append(snapshotDiskNames, GenerateSnapshotDiskName(name))
	}

	r, err := NewReadOnly(dir, snapshotDiskNames[0], snapshotDiskNames, nil)
	if err != nil {
		return err
	}

	s.replica = r

	return nil
}

func (s *SnapshotHashStatus) CloseSnapshot() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	err := s.replica.Close()
	s.replica = nil

	return err
}

func (s *SnapshotHashStatus) ReadSnapshot(start int64, data []byte) error {
	_, err := s.replica.ReadAt(data, start)
	return err
}

func (s *SnapshotHashStatus) StatSnapshot(snapshotName string) (os.FileInfo, error) {
	return os.Stat(GenerateSnapshotDiskName(snapshotName))
}

func (s *SnapshotHashStatus) GetSnapshotHashInfoFromXattr(snapshotName string) (string, string, error) {
	xattrSnapshotHashValue := make([]byte, xattrSnapshotHashValueMax)
	_, err := unix.Getxattr(GenerateSnapshotDiskName(snapshotName), xattrSnapshotHashName, xattrSnapshotHashValue)
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

func (s *SnapshotHashStatus) SetSnapshotHashInfoToXattr(snapshotName string, checksum, modTime string) error {
	xattrSnapshotHashValue, err := json.Marshal(&SnapshotXattrHashInfo{
		Checksum: checksum,
		ModTime:  modTime,
	})
	if err != nil {
		return err
	}

	return unix.Setxattr(GenerateSnapshotDiskName(snapshotName), xattrSnapshotHashName, xattrSnapshotHashValue, 0)
}

func (s *SnapshotHashStatus) GetSize() int64 {
	return s.replica.info.Size
}
