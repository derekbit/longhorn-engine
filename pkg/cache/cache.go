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
	return createCacheDevice(c.devPath, c.filePath, c.size)
}

func (c *Cache) IsSet() bool {
	if c.devPath != "" && c.filePath != "" && c.size > 0 {
		return true
	}
	return false
}

func attachLoopDevice(cacheFilePath string) (string, error) {
	dev, err := losetup.Attach(cacheFilePath, 0, false)
	if err != nil {
		return "", err
	}
	return dev.Path(), nil
}

func createCacheDevice(devPath, cacheFilePath string, cacheSize int64) error {
	devDir := filepath.Dir(devPath)
	if _, err := os.Stat(devDir); os.IsNotExist(err) {
		if err := os.MkdirAll(devDir, 0755); err != nil {
			return errors.Wrapf(err, "cannot create directory %v", devDir)
		}
	}

	cacheFile := filepath.Dir(cacheFilePath)
	if _, err := os.Stat(cacheFile); os.IsNotExist(err) {
		if err := os.MkdirAll(cacheFile, 0755); err != nil {
			return errors.Wrapf(err, "cannot create directory %v", cacheFile)
		}
	}

	if err := makeCacheFile(cacheFilePath, cacheSize); err != nil {
		return errors.Wrapf(err, "cannot create cache file %v", cacheFilePath)
	}

	loopDevPath, err := attachLoopDevice(cacheFilePath)
	if err != nil {
		return errors.Wrapf(err, "cannot attach loopback device %v", devPath)
	}

	if err := util.DuplicateDevice(loopDevPath, devPath); err != nil {
		return err
	}

	return nil
}

func makeCacheFile(filePath string, fileSize int64) error {
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

func getDevPath(volume string) string {
	return filepath.Join(DevPath, "cache", volume)
}
