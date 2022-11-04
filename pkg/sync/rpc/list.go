package rpc

import (
	"fmt"
	"sync"

	"github.com/longhorn/longhorn-engine/pkg/replica"
)

const (
	MaxBackupSize = 5

	MaxSnapshotHashTaskSize = 10
)

type BackupList struct {
	sync.RWMutex
	backups []*BackupInfo
}

type BackupInfo struct {
	backupID     string
	backupStatus *replica.BackupStatus
}

type SnapshotHashList struct {
	sync.RWMutex
	tasks []*SnapshotHashInfo
}

type SnapshotHashInfo struct {
	snapshotName string
	info         *replica.SnapshotHashTask
}

// The APIs BackupAdd, BackupGet, Refresh, BackupDelete implement the CRUD interface for the backup object
// The slice Backup.backupList is implemented similar to a FIFO queue.

// Add creates a new backupList object and appends to the end of the list maintained by backup object
func (b *BackupList) BackupAdd(backupID string, backup *replica.BackupStatus) error {
	if backupID == "" {
		return fmt.Errorf("empty backupID")
	}

	b.Lock()
	b.backups = append(b.backups, &BackupInfo{
		backupID:     backupID,
		backupStatus: backup,
	})
	b.Unlock()

	if err := b.Refresh(); err != nil {
		return err
	}

	return nil
}

// Get takes backupID input and will return the backup object corresponding to that backupID or error if not found
func (b *BackupList) BackupGet(backupID string) (*replica.BackupStatus, error) {
	if backupID == "" {
		return nil, fmt.Errorf("empty backupID")
	}

	if err := b.Refresh(); err != nil {
		return nil, err
	}

	b.RLock()
	defer b.RUnlock()

	for _, info := range b.backups {
		if info.backupID == backupID {
			return info.backupStatus, nil
		}
	}
	return nil, fmt.Errorf("backup not found %v", backupID)
}

// remove deletes the object present at slice[index] and returns the remaining elements of slice yet maintaining
// the original order of elements in the slice
func (*BackupList) remove(b []*BackupInfo, index int) ([]*BackupInfo, error) {
	if b == nil {
		return nil, fmt.Errorf("empty list")
	}
	if index >= len(b) || index < 0 {
		return nil, fmt.Errorf("BUG: attempting to delete an out of range index entry from backupList")
	}
	return append(b[:index], b[index+1:]...), nil
}

// Refresh deletes all the old completed backups from the front. Old backups are the completed backups
// that are created before MaxBackupSize completed backups
func (b *BackupList) Refresh() error {
	b.Lock()
	defer b.Unlock()

	var index, completed int

	for index = len(b.backups) - 1; index >= 0; index-- {
		if b.backups[index].backupStatus.Progress == 100 {
			if completed == MaxBackupSize {
				break
			}
			completed++
		}
	}
	if completed == MaxBackupSize {
		// Remove all the older completed backups in the range backupList[0:index]
		for ; index >= 0; index-- {
			if b.backups[index].backupStatus.Progress == 100 {
				updatedList, err := b.remove(b.backups, index)
				if err != nil {
					return err
				}
				b.backups = updatedList
				// As this backupList[index] is removed, will have to decrement the index by one
				index--
			}
		}
	}
	return nil
}

// Delete will delete the entry in the slice with the corresponding backupID
func (b *BackupList) BackupDelete(backupID string) error {
	b.Lock()
	defer b.Unlock()

	for index, backup := range b.backups {
		if backup.backupID == backupID {
			updatedList, err := b.remove(b.backups, index)
			if err != nil {
				return err
			}
			b.backups = updatedList
			return nil
		}
	}
	return fmt.Errorf("backup not found %v", backupID)
}

func (s *SnapshotHashList) Add(snapshotName string, info *replica.SnapshotHashTask) error {
	if snapshotName == "" {
		return fmt.Errorf("snapshot name is required")
	}

	s.Lock()

	for index, task := range s.tasks {
		if task.snapshotName == snapshotName {
			if task.info == nil {
				s.Unlock()
				return fmt.Errorf("BUG: snapshot %v info is nil", snapshotName)
			}

			if task.info.State == replica.ProgressStateComplete ||
				task.info.State == replica.ProgressStateError {
				updatedList, err := s.remove(s.tasks, index)
				if err != nil {
					return err
				}
				s.tasks = updatedList
				break
			} else {
				s.Unlock()
				return fmt.Errorf("hashing snapshot %v is in progress", snapshotName)
			}
		}
	}

	s.tasks = append(s.tasks, &SnapshotHashInfo{
		snapshotName: snapshotName,
		info:         info,
	})

	s.Unlock()

	if err := s.Refresh(); err != nil {
		return err
	}

	return nil
}

func (s *SnapshotHashList) Refresh() error {
	s.Lock()
	defer s.Unlock()

	if err := s.cleanup(replica.ProgressStateComplete, MaxSnapshotHashTaskSize); err != nil {
		return err
	}

	return s.cleanup(replica.ProgressStateError, MaxSnapshotHashTaskSize)
}

func (s *SnapshotHashList) cleanup(state replica.ProgressState, limit int) error {
	var index, completed int

	for index = len(s.tasks) - 1; index >= 0; index-- {
		if s.tasks[index].info.State == state {
			if completed == limit {
				break
			}
			completed++
		}
	}

	if completed == limit {
		// Remove all the older completed or error tasks in the range snapshotHashList[0:index]
		for ; index >= 0; index-- {
			if s.tasks[index].info.State == state {
				updatedList, err := s.remove(s.tasks, index)
				if err != nil {
					return err
				}
				s.tasks = updatedList
				// As this snapshotHashList[index] is removed, will have to decrement the index by one
				index--
			}
		}
	}

	return nil
}

func (s *SnapshotHashList) remove(l []*SnapshotHashInfo, index int) ([]*SnapshotHashInfo, error) {
	if l == nil {
		return nil, fmt.Errorf("empty list")
	}
	if index >= len(l) || index < 0 {
		return nil, fmt.Errorf("BUG: attempting to delete an out of range index entry from snapshotHashList")
	}
	return append(l[:index], l[index+1:]...), nil
}

func (s *SnapshotHashList) Get(snapshotName string) (*replica.SnapshotHashTask, error) {
	if snapshotName == "" {
		return nil, fmt.Errorf("snapshot name is required")
	}

	if err := s.Refresh(); err != nil {
		return nil, err
	}

	s.RLock()
	defer s.RUnlock()

	for _, task := range s.tasks {
		if task.snapshotName == snapshotName {
			return task.info, nil
		}
	}
	return nil, fmt.Errorf("snapshot %v is not found", snapshotName)
}

// Delete will delete the entry in the slice with the corresponding snapshotName
func (s *SnapshotHashList) Delete(snapshotName string) error {
	s.Lock()
	defer s.Unlock()

	for index, task := range s.tasks {
		if task.snapshotName == snapshotName {
			updatedList, err := s.remove(s.tasks, index)
			if err != nil {
				return err
			}
			s.tasks = updatedList
			return nil
		}
	}
	return nil
}

func (s *SnapshotHashList) GetSize() int {
	s.RLock()
	defer s.RUnlock()

	return len(s.tasks)
}
