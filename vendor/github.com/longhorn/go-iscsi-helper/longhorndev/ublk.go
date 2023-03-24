package longhorndev

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-iscsi-helper/ublkdev"
	"github.com/longhorn/go-iscsi-helper/util"
)

type LonghornUblkDevice struct {
	*sync.RWMutex
	name       string //VolumeName
	size       int64
	queueDepth int
	frontend   string
	endpoint   string

	ublkDevice *ublkdev.Device
}

func (ldc *LonghornDeviceCreator) NewUblkBlockDevice(name string, size int64, queueDepth int, frontend string) (DeviceService, error) {
	if name == "" || size == 0 {
		return nil, fmt.Errorf("invalid parameter for creating Longhorn device")
	}
	dev := &LonghornUblkDevice{
		RWMutex:    &sync.RWMutex{},
		name:       name,
		size:       size,
		queueDepth: queueDepth,
	}
	if err := dev.SetFrontend(frontend); err != nil {
		return nil, err
	}
	return dev, nil
}

func (d *LonghornUblkDevice) InitDevice() error {
	d.Lock()
	defer d.Unlock()

	if d.ublkDevice != nil {
		return nil
	}

	if err := d.initUblkDevice(); err != nil {
		return err
	}

	// Try to cleanup possible leftovers.
	return d.shutdownFrontend()
}

// call with lock hold
func (d *LonghornUblkDevice) initUblkDevice() error {
	dev, err := ublkdev.NewDevice(d.GetSocketPath(), d.size, d.queueDepth)
	if err != nil {
		return err
	}
	d.ublkDevice = dev
	return nil
}

func (d *LonghornUblkDevice) Start() error {
	stopCh := make(chan struct{})
	if err := <-d.WaitForSocket(stopCh); err != nil {
		return err
	}

	return d.startUblkDevice(true)
}

func (d *LonghornUblkDevice) startUblkDevice(startUblkDevice bool) (err error) {

	d.Lock()
	defer d.Unlock()

	// If ublk device is not started here, e.g., device upgrade,
	// d.scsiDevice.KernelDevice is nil.
	if startUblkDevice {
		if d.ublkDevice == nil {
			return fmt.Errorf("there is no ublk device during the frontend %v starts", d.frontend)
		}
		if err := d.ublkDevice.CreateDisk(); err != nil {
			return err
		}
		if err := d.createDev(); err != nil {
			return err
		}
		logrus.Infof("device %v: ublk device %s created", d.name, d.ublkDevice.KernelDevice.Name)
	} else {
		// TODO:
		logrus.Infof("device %v: ublk device %s reloaded the target and the initiator", d.name, d.ublkDevice.KernelDevice.Name)
	}

	d.endpoint = d.getDev()

	logrus.Debugf("device %v: frontend start succeed", d.name)

	return nil
}

func (d *LonghornUblkDevice) Shutdown() error {
	d.Lock()
	defer d.Unlock()

	if d.ublkDevice == nil {
		return nil
	}

	if err := d.shutdownFrontend(); err != nil {
		return err
	}

	d.ublkDevice = nil
	d.endpoint = ""

	return nil
}

// call with lock hold
func (d *LonghornUblkDevice) shutdownFrontend() error {
	dev := d.getDev()
	if err := util.RemoveDevice(dev); err != nil {
		return errors.Wrapf(err, "device %v: failed to remove device %s", d.name, dev)
	}

	if err := d.ublkDevice.DeleteDisk(); err != nil {
		return errors.Wrapf(err, "device %v: failed to stop ublk device", d.name)
	}

	logrus.Infof("device %v: ublk device %v shutdown", d.name, dev)

	return nil
}

func (d *LonghornUblkDevice) WaitForSocket(stopCh chan struct{}) chan error {
	errCh := make(chan error)
	go func(errCh chan error, stopCh chan struct{}) {
		socket := d.GetSocketPath()
		timeout := time.After(time.Duration(WaitCount) * WaitInterval)
		ticker := time.NewTicker(WaitInterval)
		defer ticker.Stop()
		tick := ticker.C
		for {
			select {
			case <-timeout:
				errCh <- fmt.Errorf("device %v: wait for socket %v timed out", d.name, socket)
			case <-tick:
				if _, err := os.Stat(socket); err == nil {
					errCh <- nil
					return
				}
				logrus.Infof("device %v: waiting for socket %v to show up", d.name, socket)
			case <-stopCh:
				logrus.Infof("device %v: stop wait for socket routine", d.name)
				return
			}
		}
	}(errCh, stopCh)

	return errCh
}

func (d *LonghornUblkDevice) GetSocketPath() string {
	return filepath.Join(SocketDirectory, "longhorn-"+d.name+".sock")
}

// call with lock hold
func (d *LonghornUblkDevice) getDev() string {
	return filepath.Join(DevPath, d.name)
}

// call with lock hold
func (d *LonghornUblkDevice) createDev() error {
	if _, err := os.Stat(DevPath); os.IsNotExist(err) {
		if err := os.MkdirAll(DevPath, 0755); err != nil {
			logrus.Fatalf("device %v: cannot create directory %v", d.name, DevPath)
		}
	}

	dev := d.getDev()
	if _, err := os.Stat(dev); err == nil {
		logrus.Warnf("Device %s already exists, clean it up", dev)
		if err := util.RemoveDevice(dev); err != nil {
			return errors.Wrapf(err, "cannot clean up block device file %v", dev)
		}
	}

	if err := util.DuplicateDevice(d.ublkDevice.KernelDevice, dev); err != nil {
		return err
	}

	logrus.Debugf("device %v: Device %s is ready", d.name, dev)

	return nil
}

func (d *LonghornUblkDevice) PrepareUpgrade() error {
	if d.frontend == "" {
		return nil
	}

	if err := util.RemoveFile(d.GetSocketPath()); err != nil {
		return errors.Wrapf(err, "failed to remove socket %v", d.GetSocketPath())
	}
	return nil
}

func (d *LonghornUblkDevice) FinishUpgrade() (err error) {
	if d.frontend == "" {
		return nil
	}

	stopCh := make(chan struct{})
	socketError := d.WaitForSocket(stopCh)
	select {
	case err = <-socketError:
		if err != nil {
			logrus.Errorf("error waiting for the socket %v", err)
			err = errors.Wrapf(err, "error waiting for the socket")
		}
		break
	}
	close(stopCh)
	close(socketError)

	if err != nil {
		return err
	}

	// TODO: Need to fix `ReloadSocketConnection` since it doesn't work for frontend `FrontendTGTISCSI`.
	if err := d.ReloadSocketConnection(); err != nil {
		return err
	}

	d.Lock()
	if err := d.initUblkDevice(); err != nil {
		d.Unlock()
		return err
	}
	d.Unlock()

	return d.startUblkDevice(false)
}

func (d *LonghornUblkDevice) ReloadSocketConnection() error {
	d.RLock()
	dev := d.getDev()
	d.RUnlock()

	cmd := exec.Command("sg_raw", dev, "a6", "00", "00", "00", "00", "00")
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "failed to reload socket connection at %v", dev)
	}
	logrus.Infof("Reloaded completed for device %v", dev)
	return nil
}

func (d *LonghornUblkDevice) SetFrontend(frontend string) error {
	d.Lock()
	defer d.Unlock()

	if d.frontend != "" {
		if d.frontend != frontend {
			return fmt.Errorf("engine frontend %v is already up and cannot be set to %v", d.frontend, frontend)
		}
		if d.ublkDevice != nil {
			logrus.Infof("Engine frontend %v is already up", frontend)
			return nil
		}
		return fmt.Errorf("engine frontend had been set to %v, but its frontend cannot be started before engine manager shutdown its frontend", frontend)
	}

	if d.ublkDevice != nil {
		return fmt.Errorf("BUG: engine launcher frontend is empty but scsi device hasn't been cleanup in frontend start")
	}

	d.frontend = frontend

	return nil
}

func (d *LonghornUblkDevice) UnsetFrontendCheck() error {
	d.Lock()
	defer d.Unlock()

	if d.ublkDevice == nil {
		d.frontend = ""
		logrus.Debugf("Engine frontend is already down")
		return nil
	}

	if d.frontend == "" {
		return fmt.Errorf("BUG: engine launcher frontend is empty but scsi device hasn't been cleanup in frontend shutdown")
	}
	return nil
}

func (d *LonghornUblkDevice) UnsetFrontend() {
	d.Lock()
	defer d.Unlock()

	d.frontend = ""
}

func (d *LonghornUblkDevice) Enabled() bool {
	d.RLock()
	defer d.RUnlock()
	return d.ublkDevice != nil
}

func (d *LonghornUblkDevice) GetEndpoint() string {
	d.RLock()
	defer d.RUnlock()
	return d.endpoint
}

func (d *LonghornUblkDevice) GetFrontend() string {
	d.RLock()
	defer d.RUnlock()
	return d.frontend
}

func (d *LonghornUblkDevice) Expand(size int64) (err error) {
	return nil
}
