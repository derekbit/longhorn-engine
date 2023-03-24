package ublk

import (
	"github.com/longhorn/go-iscsi-helper/longhorndev"
	"github.com/longhorn/longhorn-engine/pkg/frontend/socket"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/sirupsen/logrus"
)

const (
	DevPath = "/dev/longhorn/"
)

type Ublk struct {
	s *socket.Socket

	isUp         bool
	dev          longhorndev.DeviceService
	frontendName string
	queueDepth   int
}

func New(frontendName string, queueDepth int) types.Frontend {
	s := socket.New()
	return &Ublk{s, false, nil, frontendName, queueDepth}
}

func (t *Ublk) FrontendName() string {
	return t.frontendName
}

func (t *Ublk) Init(name string, size, sectorSize int64) error {
	if err := t.s.Init(name, size, sectorSize); err != nil {
		return err
	}

	ldc := longhorndev.LonghornDeviceCreator{}

	var dev longhorndev.DeviceService
	var err error

	logrus.Info("Initializing ublk device")
	dev, err = ldc.NewUblkBlockDevice(name, size, t.queueDepth, t.frontendName)
	if err != nil {
		return err
	}
	t.dev = dev

	if err := t.dev.InitDevice(); err != nil {
		return err
	}

	t.isUp = false
	return nil
}

func (t *Ublk) Startup(rwu types.ReaderWriterUnmapperAt) error {
	if err := t.s.Startup(rwu); err != nil {
		return err
	}

	if err := t.dev.Start(); err != nil {
		return err
	}

	t.isUp = true

	return nil
}

func (t *Ublk) Shutdown() error {
	if t.dev != nil {
		if err := t.dev.Shutdown(); err != nil {
			return err
		}
	}
	if err := t.s.Shutdown(); err != nil {
		return err
	}
	t.isUp = false

	return nil
}

func (t *Ublk) State() types.State {
	if t.isUp {
		return types.StateUp
	}
	return types.StateDown
}

func (t *Ublk) Endpoint() string {
	if t.isUp {
		return t.dev.GetEndpoint()
	}
	return ""
}

func (t *Ublk) Upgrade(name string, size, sectorSize int64, rwu types.ReaderWriterUnmapperAt) error {
	return nil
}

func (t *Ublk) Expand(size int64) error {
	return nil
}
