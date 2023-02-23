package util

import (
	"fmt"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type FifoAddr struct {
	Name string
	Net  string
}

func (a *FifoAddr) Network() string {
	return a.Net
}

func (a *FifoAddr) String() string {
	if a == nil {
		return "<nil>"
	}
	return a.Name
}

// Server role (replica)
// request path: replica receives requests from engine
// response path: replica sends responses to engine
type FifoServerConn struct {
	address  string
	request  *os.File
	response *os.File
}

func ListenFifo(address string) (net.Conn, error) {
	requestPath := address + ".request"
	responsePath := address + ".response"

	for _, path := range []string{requestPath, responsePath} {
		info, err := os.Stat(path)
		if err != nil {
			if err := syscall.Mkfifo(path, 0600); err != nil {
				return nil, errors.Wrapf(err, "failed to create a named pipe at %v", path)
			}
		} else {
			if (info.Mode() & os.ModeNamedPipe) == 0 {
				return nil, fmt.Errorf("file %v already exists and is not a named pipe", path)
			}
		}
	}

	var request, response *os.File
	var err error

	defer func() {
		if err != nil {
			if request != nil {
				request.Close()
			}
			if response != nil {
				response.Close()
			}
		}
	}()

	request, err = os.OpenFile(requestPath, os.O_RDONLY, 0600)
	if err != nil {
		return nil, err
	}
	response, err = os.OpenFile(responsePath, os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}

	return &FifoServerConn{
		address:  address,
		request:  request,
		response: response,
	}, nil
}

func (f *FifoServerConn) Read(buf []byte) (int, error) {
	return f.request.Read(buf)
}

func (f *FifoServerConn) SetReadDeadline(t time.Time) error {
	return f.request.SetReadDeadline(t)
}

func (f *FifoServerConn) Write(buf []byte) (int, error) {
	return f.response.Write(buf)
}

func (f *FifoServerConn) SetWriteDeadline(t time.Time) error {
	return f.response.SetWriteDeadline(t)
}

func (f *FifoServerConn) SetDeadline(t time.Time) error {
	if f.request != nil {
		if err := f.request.SetDeadline(t); err != nil {
			return err
		}
	}
	if f.response != nil {
		if err := f.response.SetDeadline(t); err != nil {
			return err
		}
	}
	return nil
}

func (f *FifoServerConn) Close() error {
	if f.request != nil {
		f.request.Close()
		f.request = nil
	}
	if f.response != nil {
		f.response.Close()
		f.response = nil
	}
	return nil
}

func (f *FifoServerConn) LocalAddr() net.Addr {
	return &FifoAddr{
		Name: f.address,
		Net:  "fifo",
	}
}

func (f *FifoServerConn) RemoteAddr() net.Addr {
	return &FifoAddr{
		Name: f.address,
		Net:  "fifo",
	}
}

// Client role (engine)
// request path: engine sends requests to replica
// response path: engine receives responses from replica
type FifoClientConn struct {
	address  string
	request  *os.File
	response *os.File
}

func DialFifo(address string) (net.Conn, error) {
	logrus.Infof("Dialing to FIFO %v", address)

	requestPath := address + ".request"
	responsePath := address + ".response"

	for _, path := range []string{requestPath, responsePath} {
		info, err := os.Stat(path)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get file info of a named pipe at %v", path)
		}
		if (info.Mode() & os.ModeNamedPipe) == 0 {
			return nil, fmt.Errorf("file %v already exists and is not a named pipe", path)
		}
	}

	var request, response *os.File
	var err error

	defer func() {
		if err != nil {
			if request != nil {
				request.Close()
			}
			if response != nil {
				response.Close()
			}
		}
	}()

	request, err = os.OpenFile(requestPath, os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	response, err = os.OpenFile(responsePath, os.O_RDONLY, 0600)
	if err != nil {
		return nil, err
	}

	return &FifoClientConn{
		address:  address,
		request:  request,
		response: response,
	}, nil
}

func (f *FifoClientConn) Read(buf []byte) (int, error) {
	return f.response.Read(buf)
}

func (f *FifoClientConn) SetReadDeadline(t time.Time) error {
	return f.response.SetReadDeadline(t)
}

func (f *FifoClientConn) Write(buf []byte) (int, error) {
	return f.request.Write(buf)
}

func (f *FifoClientConn) SetWriteDeadline(t time.Time) error {
	return f.request.SetWriteDeadline(t)
}

func (f *FifoClientConn) SetDeadline(t time.Time) error {
	if f.request != nil {
		if err := f.request.SetDeadline(t); err != nil {
			return err
		}
	}
	if f.response != nil {
		if err := f.response.SetDeadline(t); err != nil {
			return err
		}
	}
	return nil
}

func (f *FifoClientConn) Close() error {
	if f.request != nil {
		f.request.Close()
		f.request = nil
	}
	if f.response != nil {
		f.response.Close()
		f.response = nil
	}
	return nil
}

func (f *FifoClientConn) LocalAddr() net.Addr {
	return &FifoAddr{
		Name: f.address,
		Net:  "fifo",
	}
}

func (f *FifoClientConn) RemoteAddr() net.Addr {
	return &FifoAddr{
		Name: f.address,
		Net:  "fifo",
	}
}
