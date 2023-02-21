package dataconn

import (
	"github.com/longhorn/longhorn-engine/pkg/backingfile"
)

// Client replica client
type LocalClient struct {
}

// NewLocalClient replica client using network protocol
func NewLocalClient(dir string, backing *backingfile.BackingFile, sectorSize int64, disableRevCounter, unmapMarkDiskChainRemoved bool) (DataConnClient, error) {
	return &LocalClient{}, nil
}

// ReadAt replica client
func (c *LocalClient) ReadAt(buf []byte, offset int64) (int, error) {
	return 0, nil
}

// WriteAt replica client
func (c *LocalClient) WriteAt(buf []byte, offset int64) (int, error) {
	return 0, nil
}

// UnmapAt replica client
func (c *LocalClient) UnmapAt(length uint32, offset int64) (int, error) {
	return 0, nil
}

// Close replica client
func (c *LocalClient) Close() {
}

// Ping replica client
func (c *LocalClient) Ping() error {
	return nil
}

// SetError replica client transport error
func (c *LocalClient) SetError(err error) {
}
