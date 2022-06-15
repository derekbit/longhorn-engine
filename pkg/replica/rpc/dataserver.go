package rpc

import (
	"net"
	"path/filepath"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/dataconn"
	"github.com/longhorn/longhorn-engine/pkg/replica"
)

type DataServer struct {
	address string
	s       *replica.Server
}

func NewDataServer(address string, s *replica.Server) *DataServer {
	return &DataServer{
		address: address,
		s:       s,
	}
}

func (s *DataServer) ListenAndServe() error {
	sockPath := filepath.Join("/host/var/lib/longhorn/uds", "example.sock")
	uaddr, err := net.ResolveUnixAddr("unix", sockPath)
	if err != nil {
		return err
	}

	l, err := net.ListenUnix("unix", uaddr)
	if err != nil {
		return err
	}

	for {
		conn, err := l.AcceptUnix()
		if err != nil {
			logrus.Errorf("failed to accept connection %v", err)
			continue
		}

		logrus.Infof("New connection from: %v", conn.RemoteAddr())

		go func(conn net.Conn) {
			server := dataconn.NewServer(conn, s.s)
			server.Handle()
		}(conn)
	}
}
