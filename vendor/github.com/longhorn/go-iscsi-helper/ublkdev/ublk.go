package ublkdev

import (
	"github.com/longhorn/go-iscsi-helper/ublk"
	"github.com/longhorn/go-iscsi-helper/util"
)

var (
	hostProc = "/host/proc"
)

type Device struct {
	DevId        int
	KernelDevice *util.KernelDevice
	BackingFile  string
	Size         int64
	QueueDepth   int
}

func NewDevice(backingFile string, size int64, queueDepth int) (*Device, error) {
	return &Device{
		BackingFile: backingFile,
		Size:        size,
		QueueDepth:  queueDepth,
	}, nil
}

func (dev *Device) CreateDisk() error {
	ne, err := util.NewNamespaceExecutor(util.GetHostNamespacePath(hostProc))
	if err != nil {
		return err
	}

	devId, err := ublk.StartDaemon(dev.BackingFile, dev.Size, dev.QueueDepth, ne)
	if err != nil {
		return err
	}
	dev.DevId = devId

	dev.KernelDevice, err = ublk.GetDevice(devId, ne)
	if err != nil {
		return err
	}

	return nil
}

func (dev *Device) DeleteDisk() error {
	return ublk.ShutdownDaemon(dev.DevId)
}
