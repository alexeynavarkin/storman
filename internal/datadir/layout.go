package datadir

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/alexnav/storman/internal/config"
)

// Bootstrap creates the fixed layout under dataDir and writes a fresh config.json
// if one does not already exist. It is idempotent: running it on an already
// initialized directory is a no-op (existing config.json is preserved).
//
// Returns true if a new config was generated, false if the dir was already initialized.
func Bootstrap(dataDir string) (created bool, err error) {
	abs, err := filepath.Abs(dataDir)
	if err != nil {
		return false, fmt.Errorf("resolve data_dir: %w", err)
	}

	for _, dir := range []string{abs, StorageDir(abs), MetaDir(abs), TrashDir(abs), UploadsDir(abs), BackupsDir(abs)} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return false, fmt.Errorf("mkdir %s: %w", dir, err)
		}
	}

	cfgPath := ConfigPath(abs)
	if _, err := os.Stat(cfgPath); err == nil {
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("stat config: %w", err)
	}

	key := make([]byte, config.SecretsKeyLen)
	if _, err := rand.Read(key); err != nil {
		return false, fmt.Errorf("generate secrets_key: %w", err)
	}
	cfg := config.Default(abs)
	cfg.SecretsKey = base64.StdEncoding.EncodeToString(key)
	if err := config.Save(cfgPath, cfg); err != nil {
		return false, fmt.Errorf("write config: %w", err)
	}
	return true, nil
}
