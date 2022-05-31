package dataconn

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"unsafe"
)

const (
	requestHeaderSize = 26
)

type Wire struct {
	conn   net.Conn
	writer *bufio.Writer
	reader io.Reader
}

func NewWire(conn net.Conn) *Wire {
	return &Wire{
		conn:   conn,
		writer: bufio.NewWriterSize(conn, writeBufferSize),
		reader: bufio.NewReaderSize(conn, readBufferSize),
	}
}

func (w *Wire) Write(msg *Message) error {
	header := make([]byte, requestHeaderSize)
	offset := 0

	binary.LittleEndian.PutUint16(header[offset:], msg.MagicVersion)
	offset += int(unsafe.Sizeof(msg.MagicVersion))

	binary.LittleEndian.PutUint32(header[offset:], msg.Seq)
	offset += int(unsafe.Sizeof(msg.Seq))

	binary.LittleEndian.PutUint32(header[offset:], msg.Type)
	offset += int(unsafe.Sizeof(msg.Type))

	binary.LittleEndian.PutUint64(header[offset:], uint64(msg.Offset))
	offset += int(unsafe.Sizeof(msg.Offset))

	binary.LittleEndian.PutUint32(header[offset:], msg.Size)
	offset += int(unsafe.Sizeof(msg.Size))

	binary.LittleEndian.PutUint32(header[offset:], uint32(len(msg.Data)))

	if _, err := w.writer.Write(header); err != nil {
		return err
	}

	if len(msg.Data) > 0 {
		if _, err := w.writer.Write(msg.Data); err != nil {
			return err
		}
	}
	return w.writer.Flush()
}

func (w *Wire) Read() (*Message, error) {
	var (
		msg    Message
		length uint32
	)

	header := make([]byte, requestHeaderSize)
	offset := 0

	if _, err := io.ReadFull(w.reader, header); err != nil {
		return nil, err
	}

	msg.MagicVersion = binary.LittleEndian.Uint16(header[offset:])
	if msg.MagicVersion != MagicVersion {
		return nil, fmt.Errorf("wrong API version received: 0x%x", &msg.MagicVersion)
	}
	offset += int(unsafe.Sizeof(msg.MagicVersion))

	msg.Seq = binary.LittleEndian.Uint32(header[offset:])
	offset += int(unsafe.Sizeof(msg.Seq))

	msg.Type = binary.LittleEndian.Uint32(header[offset:])
	offset += int(unsafe.Sizeof(msg.Type))

	msg.Offset = int64(binary.LittleEndian.Uint64(header[offset:]))
	offset += int(unsafe.Sizeof(msg.Offset))

	msg.Size = binary.LittleEndian.Uint32(header[offset:])
	offset += int(unsafe.Sizeof(msg.Size))

	length = binary.LittleEndian.Uint32(header[offset:])
	if length > 0 {
		msg.Data = make([]byte, length)
		if _, err := io.ReadFull(w.reader, msg.Data); err != nil {
			return nil, err
		}
	}

	return &msg, nil
}

func (w *Wire) Close() error {
	return w.conn.Close()
}
