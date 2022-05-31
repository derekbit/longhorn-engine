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
	logrus.Infof("Debug ========>  server s.address=%v", s.address)
	/*
		addr, err := net.ResolveTCPAddr("tcp", s.address)
		if err != nil {
			return err
		}

		l, err := net.ListenTCP("tcp", addr)
		if err != nil {
			return err
		}
	*/

	sockPath := filepath.Join("/uds", "example.sock")
	uaddr, err := net.ResolveUnixAddr("unix", sockPath)
	if err != nil {
		return err
	}

	l, err := net.ListenUnix("unix", uaddr)
	logrus.Infof("Debug --------> path=%v err=%v", sockPath, err)
	if err != nil {
		return err
	}

	for {
		//conn, err := l.AcceptTCP()
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
