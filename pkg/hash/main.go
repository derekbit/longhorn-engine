package hash

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/replica"
)

const (
	defaultBlockSize = 2097152
)

func DoHashSnapshot(snapshotNames []string, rehash bool) *replica.SnapshotHashStatus {
	status := replica.NewSnapshotHash(snapshotNames)

	go func() {
		hashSnapshot(status, snapshotNames, rehash)
	}()

	return status
}

func hashSnapshot(status *replica.SnapshotHashStatus, snapshotNames []string, rehash bool) {
	var err error
	var checksum string

	defer func() {
		status.Checksum = checksum
		if err != nil {
			status.State = replica.ProgressStateError
		} else {
			status.State = replica.ProgressStateComplete
		}

		logrus.Infof("Snapshot %v checksum %v", snapshotNames, checksum)
	}()

	checksum, err = doHashSnapshot(status, snapshotNames)
	if err != nil {
		logrus.Errorf("failed to hash snapshot %v since %v", snapshotNames, err)
	}
}

func doHashSnapshot(status *replica.SnapshotHashStatus, snapshotNames []string) (string, error) {
	err := status.OpenSnapshot(snapshotNames)
	if err != nil {
		logrus.Errorf("failed to open snapshot %v since %v", snapshotNames, err)
		return "", err
	}
	defer status.CloseSnapshot()

	h := sha256.New()

	size := status.GetSize()
	blkCounts := size / defaultBlockSize
	blkBuff := make([]byte, defaultBlockSize)

	for i := int64(0); i < blkCounts; i++ {
		offset := i * defaultBlockSize
		if err := status.ReadSnapshot(offset, blkBuff); err != nil {
			return "", err
		}
		written, err := h.Write(blkBuff)
		if err != nil {
			return "", err
		}

		status.Progress = int(100 * float64(offset+int64(written)) / float64(size))
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}
