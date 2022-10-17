package replica

import (
	"os"
	"sync"

	"github.com/pkg/errors"
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

func (s *SnapshotHashStatus) GetSize() int64 {
	return s.replica.info.Size
}
