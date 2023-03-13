package longhorndev

import "time"

const (
	SocketDirectory = "/var/run/longhorn"
	DevPath         = "/dev/longhorn/"

	WaitInterval = time.Second
	WaitCount    = 30
)

type DeviceService interface {
	GetFrontend() string
	SetFrontend(frontend string) error
	UnsetFrontendCheck() error
	UnsetFrontend()
	GetEndpoint() string
	Enabled() bool

	InitDevice() error
	Start() error
	Shutdown() error
	PrepareUpgrade() error
	FinishUpgrade() error
	Expand(size int64) error
}

type DeviceCreator interface {
	NewDevice(name string, size int64, frontend string) (DeviceService, error)
}

type LonghornDeviceCreator struct{}
