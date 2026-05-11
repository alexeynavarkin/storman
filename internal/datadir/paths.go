// Package datadir owns the on-disk layout under a storman data directory.
//
// Layout (fixed, not configurable — see docs/arch/overview.md):
//
//	<data-dir>/
//	  config.json
//	  flat-storage/        — user files as-is (storage-dir)
//	  meta-storage/
//	    trash/
//	    uploads/
//	    backups/
package datadir

import "path/filepath"

const (
	flatStorage = "flat-storage"
	metaStorage = "meta-storage"
	trashSub    = "trash"
	uploadsSub  = "uploads"
	backupsSub  = "backups"
	configFile  = "config.json"
)

func ConfigPath(dataDir string) string  { return filepath.Join(dataDir, configFile) }
func StorageDir(dataDir string) string  { return filepath.Join(dataDir, flatStorage) }
func MetaDir(dataDir string) string     { return filepath.Join(dataDir, metaStorage) }
func TrashDir(dataDir string) string    { return filepath.Join(MetaDir(dataDir), trashSub) }
func UploadsDir(dataDir string) string  { return filepath.Join(MetaDir(dataDir), uploadsSub) }
func BackupsDir(dataDir string) string  { return filepath.Join(MetaDir(dataDir), backupsSub) }
