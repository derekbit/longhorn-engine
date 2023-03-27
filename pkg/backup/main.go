package backup

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/backupstore"

	"github.com/longhorn/longhorn-engine/pkg/backingfile"
	"github.com/longhorn/longhorn-engine/pkg/replica"
	"github.com/longhorn/longhorn-engine/pkg/util"
)

var (
	VERSION = "0.0.0"
	log     = logrus.WithFields(logrus.Fields{"pkg": "backup"})
)

type ErrorResponse struct {
	Error string
}

func ResponseLogAndError(v interface{}) {
	if e, ok := v.(*logrus.Entry); ok {
		e.Error(e.Message)
		fmt.Println(e.Message)
	} else {
		e, isErr := v.(error)
		_, isRuntimeErr := e.(runtime.Error)
		if isErr && !isRuntimeErr {
			logrus.Errorf(fmt.Sprint(e))
			fmt.Println(fmt.Sprint(e))
		} else {
			logrus.Errorf("Caught FATAL error: %s", v)
			debug.PrintStack()
			fmt.Println("Caught FATAL error: ", v)
		}
	}
}

// ResponseOutput would generate a JSON format byte array of object for output
func ResponseOutput(v interface{}) ([]byte, error) {
	j, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return nil, err
	}
	return j, nil
}

func RequiredMissingError(name string) error {
	return fmt.Errorf("cannot find valid required parameter: %v", name)
}

func BackupInit(backupName, volumeName, snapshotName, destURL, backingImageName, backingImageChecksum string,
	labels []string) (*replica.BackupStatus, *backupstore.DeltaBackupConfig, error) {
	var (
		err         error
		backingFile *backingfile.BackingFile
		labelMap    map[string]string
	)

	if volumeName == "" || snapshotName == "" || destURL == "" {
		return nil, nil, fmt.Errorf("missing input parameter")
	}

	if !util.ValidVolumeName(volumeName) {
		return nil, nil, fmt.Errorf("invalid volume name %v for backup", volumeName)
	}

	if labels != nil {
		labelMap, err = util.ParseLabels(labels)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot parse backup labels")
		}
	}

	dir, err := os.Getwd()
	if err != nil {
		return nil, nil, err
	}

	volumeInfo, err := replica.ReadInfo(dir)
	if err != nil {
		return nil, nil, err
	}
	if volumeInfo.BackingFilePath != "" {
		backingFilePath := volumeInfo.BackingFilePath
		if _, err := os.Stat(backingFilePath); err != nil {
			return nil, nil, err
		}

		backingFile, err = backingfile.OpenBackingFile(backingFilePath)
		if err != nil {
			return nil, nil, err
		}
	}

	volume := &backupstore.Volume{
		Name:                 volumeName,
		Size:                 volumeInfo.Size,
		Labels:               labelMap,
		BackingImageName:     backingImageName,
		BackingImageChecksum: backingImageChecksum,
		CreatedTime:          util.Now(),
	}

	snapshot := &backupstore.Snapshot{
		Name:        snapshotName,
		CreatedTime: util.Now(),
	}

	originalBackupName := backupName
	if backupName == "" {
		backupName = util.GenerateName("backup")
	}

	replicaBackup := replica.NewBackup(backupName, backingFile)

	config := &backupstore.DeltaBackupConfig{
		BackupName: originalBackupName,
		Volume:     volume,
		Snapshot:   snapshot,
		DestURL:    destURL,
		DeltaOps:   replicaBackup,
		Labels:     labelMap,
	}

	return replicaBackup, config, nil
}

func DoBackupCreate(replicaBackup *replica.BackupStatus, config *backupstore.DeltaBackupConfig) error {
	isIncremental, err := backupstore.CreateDeltaBlockBackup(replicaBackup.Name, config)
	if err != nil {
		return err
	}
	replicaBackup.IsIncremental = isIncremental
	return nil
}

func DoBackupRestore(backupURL string, toFile string, restoreObj *replica.RestoreStatus) error {
	backupURL = util.UnescapeURL(backupURL)
	log.Debugf("Start restoring from %v into snapshot %v", backupURL, toFile)

	config := &backupstore.DeltaRestoreConfig{
		BackupURL: backupURL,
		DeltaOps:  restoreObj,
		Filename:  toFile,
	}

	if err := backupstore.RestoreDeltaBlockBackup(config); err != nil {
		return err
	}

	return nil
}

func DoBackupRestoreIncrementally(url string, deltaFile string, lastRestored string,
	restoreObj *replica.RestoreStatus) error {
	backupURL := util.UnescapeURL(url)
	log.Debugf("Start incremental restoring from %v into delta file %v", backupURL, deltaFile)

	config := &backupstore.DeltaRestoreConfig{
		BackupURL:      backupURL,
		DeltaOps:       restoreObj,
		LastBackupName: lastRestored,
		Filename:       deltaFile,
	}

	if err := backupstore.RestoreDeltaBlockBackupIncrementally(config); err != nil {
		return err
	}

	return nil
}

func CreateNewSnapshotMetafile(file string) error {
	f, err := os.Create(file + ".tmp")
	if err != nil {
		return err
	}
	defer f.Close()

	content := "{\"Parent\":\"\"}\n"
	if _, err := f.Write([]byte(content)); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(file+".tmp", file)
}
