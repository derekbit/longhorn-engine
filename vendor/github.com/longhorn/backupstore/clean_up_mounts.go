package backupstore

import (
	"os"

	"github.com/longhorn/backupstore/util"
	mount "k8s.io/mount-utils"
)

func CleanUpMounts(inUseBackupTargets []string) (err error) {
	mounter := mount.New("")

	if _, err := os.Stat(util.MountDir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	return util.CleanUpMountPoints(mounter, inUseBackupTargets, log)
}
