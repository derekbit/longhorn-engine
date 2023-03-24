package ublk

import (
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-iscsi-helper/util"
)

var (
	DeviceWaitRetryCounts   = 10
	DeviceWaitRetryInterval = 1 * time.Second
)

const (
	hostProc = "/host/proc"
)

func parseDeviceId(str string) (int, error) {
	re := regexp.MustCompile(`dev id (\d+)`)
	match := re.FindStringSubmatch(str)
	if len(match) > 1 {
		return strconv.Atoi(match[1])
	}

	return -1, fmt.Errorf("failed to parse the device id from \"%v\"", str)
}

// StartDaemon will start ublk server daemon, prepare for further commands
func StartDaemon(backingFile string, size int64, queueDepth int, ne *util.NamespaceExecutor) (int, error) {
	opts := []string{
		"add",
		"-t",
		"longhorn",
		"-f",
		backingFile,
		"-s",
		strconv.FormatInt(size, 10),
		"-d",
		strconv.Itoa(queueDepth),
		"-q",
		"1",
	}

	output, err := ne.Execute("ublk", opts)
	if err != nil {
		logrus.Errorf("go-iscsi-helper: command failed: %v", err)
		return -1, err
	}

	logrus.Info("go-iscsi-helper: done")

	return parseDeviceId(output)
}

func ShutdownDaemon(devId int) error {
	ne, err := util.NewNamespaceExecutor(util.GetHostNamespacePath(hostProc))
	if err != nil {
		return err
	}

	opts := []string{
		"del",
		"-n",
		strconv.Itoa(devId),
	}
	_, err = ne.Execute("ublk", opts)
	return nil
}

func GetDevice(devId int, ne *util.NamespaceExecutor) (*util.KernelDevice, error) {
	// now that we know the device is mapped, we can get it's (major:minor)
	devices, err := util.GetKnownDevices(ne)
	if err != nil {
		return nil, err
	}

	name := "ublkb" + strconv.Itoa(devId)

	dev, known := devices[name]
	if !known {
		return nil, fmt.Errorf("cannot find kernel device for ublk device: %s", name)
	}

	return dev, nil
}
