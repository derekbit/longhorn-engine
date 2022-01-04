package cache

import (
	"os"
	"path/filepath"
	"syscall"

	"github.com/freddierice/go-losetup"
	"github.com/longhorn/longhorn-engine/pkg/util"
	"github.com/sirupsen/logrus"

	"github.com/pkg/errors"
)

const (
	DevPath            = "/dev/longhorn/"
	DeviceFormatString = "/dev/loop%d"
)

type Cache struct {
	dev      losetup.Device
	devPath  string
	filePath string
	size     int64
}

func NewCache(volume, filePath string, size int64) *Cache {
	return &Cache{
		devPath:  getDevPath(volume),
		filePath: filePath,
		size:     size,
	}
}

func (c *Cache) Start() error {
	logrus.Infof("Create cache device %v from %v with size %v bytes", c.devPath, c.filePath, c.size)

	devDir := filepath.Dir(c.devPath)
	if _, err := os.Stat(devDir); os.IsNotExist(err) {
		if err := os.MkdirAll(devDir, 0755); err != nil {
			return errors.Wrapf(err, "cannot create directory %v", devDir)
		}
	}

	cacheFile := filepath.Dir(c.filePath)
	if _, err := os.Stat(cacheFile); os.IsNotExist(err) {
		if err := os.MkdirAll(cacheFile, 0755); err != nil {
			return errors.Wrapf(err, "cannot create directory %v", cacheFile)
		}
	}

	if err := createCacheFile(c.filePath, c.size); err != nil {
		return errors.Wrapf(err, "cannot create cache file %v", c.filePath)
	}

	loopDev, err := losetup.Attach(c.filePath, 0, false)
	if err != nil {
		return errors.Wrapf(err, "cannot attach loopback device %v", c.devPath)
	}

	c.dev = loopDev

	if err := util.DuplicateDevice(loopDev.Path(), c.devPath); err != nil {
		return errors.Wrapf(err, "cannot duplicate device %v", c.devPath)
	}

	return nil
}

func (c *Cache) Shutdown() error {
	logrus.Infof("Debug =========> RemoveDevice")
	if err := util.RemoveDevice(c.devPath); err != nil {
		return errors.Wrapf(err, "cannot remove device %v", c.devPath)
	}

	logrus.Infof("Debug =========> Detach")
	if err := c.dev.Detach(); err != nil {
		return errors.Wrapf(err, "cannot detach device %v", c.dev.Path())
	}

	logrus.Infof("Debug =========> Remove")
	if err := removeCacheFile(c.filePath); err != nil {
		return errors.Wrapf(err, "cannot delete cacheFile %v", c.filePath)
	}
	return nil
}

func (c *Cache) IsSet() bool {
	if c.devPath != "" && c.filePath != "" && c.size > 0 {
		return true
	}
	return false
}

func createCacheFile(filePath string, fileSize int64) error {
	if _, err := os.Stat(filePath); err == nil {
		if err := os.Remove(filePath); err != nil {
			return err
		}
	}

	f, err := os.Create(filePath)
	defer f.Close()
	if err != nil {
		return err
	}

	if err := syscall.Fallocate(int(f.Fd()), 0, 0, fileSize); err != nil {
		return err
	}

	return nil
}

func removeCacheFile(filePath string) error {
	if _, err := os.Stat(filePath); err == nil {
		if err := os.Remove(filePath); err != nil {
			return err
		}
	}
	return nil
}

func getDevPath(volume string) string {
	return filepath.Join(DevPath, "cache", volume)
}
