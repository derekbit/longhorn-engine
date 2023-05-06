package cmd

import (
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/backupstore"
)

func BackupCleanUpMountsCmd() cli.Command {
	return cli.Command{
		Name:  "clean-up-mounts",
		Usage: "clean up mount points except for the in-use ones",
		Flags: []cli.Flag{
			cli.StringSliceFlag{
				Name:  "in-use-backup-targets",
				Usage: "in-use backup targets",
				Value: (*cli.StringSlice)(&[]string{""}),
			},
		},
		Action: cmdCleanUpMounts,
	}
}

func cmdCleanUpMounts(c *cli.Context) {
	if err := doCleanUpMounts(c); err != nil {
		panic(err)
	}
}

func doCleanUpMounts(c *cli.Context) error {
	log := logrus.WithFields(logrus.Fields{"Command": "clean-up-mounts"})

	inUseBackupTargets := c.StringSlice("in-use-backup-targets")

	log.Infof("Start to clean up mount points except for the in-use ones: %v", inUseBackupTargets)

	if err := backupstore.CleanUpMounts(inUseBackupTargets); err != nil {
		log.WithError(err).Warnf("Failed to clean up mount points")
		return err
	}

	return nil
}
