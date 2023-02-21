package dataconn

import (
	"net"
	"time"

	"github.com/longhorn/longhorn-engine/pkg/types"
)

type DataConnClient interface {
	types.ReaderWriterUnmapperAt
	Close()
	Ping() error
	SetError(err error)
}

// NewClient replica client
func NewClient(conn net.Conn, engineToReplicaTimeout time.Duration) (DataConnClient, error) {
	return NewRemoteClient(conn, engineToReplicaTimeout)
}
