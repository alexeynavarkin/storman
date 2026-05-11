// Package config defines the on-disk configuration for a storman data directory.
package config

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

const SecretsKeyLen = 32

type Config struct {
	DataDir    string         `json:"data_dir"`
	Database   DatabaseConfig `json:"database"`
	SecretsKey string         `json:"secrets_key"`
	Backup     BackupConfig   `json:"backup"`
	Web        WebConfig      `json:"web"`
	Trash      TrashConfig    `json:"trash"`
	FTP        FTPConfig      `json:"ftp"`
	Indexing   IndexingConfig `json:"indexing"`
	Tus        TusConfig      `json:"tus"`
	WebDAV     WebDAVConfig   `json:"webdav"`
}

// WebDAVConfig controls the WebDAV interface mounted under the main HTTPS
// server. Auth is HTTP Basic with an app-password (same secret a user would
// use for FTPS — see auth.UserService.AuthenticateAppPassword). PathPrefix
// defaults to "/dav" when empty; never serve WebDAV without TLS in production
// — Basic credentials travel in clear.
type WebDAVConfig struct {
	Enabled    bool   `json:"enabled"`
	PathPrefix string `json:"path_prefix,omitempty"`
}

// IndexingConfig tunes the async job pool (docs/arch/indexing.md). 0 in either
// field falls back to package defaults (2 workers / 5s).
type IndexingConfig struct {
	Workers      int      `json:"workers"`
	PollInterval Duration `json:"poll_interval"`
}

// TusConfig tunes the tus.io upload sweeper. tus uploads parked under
// meta-storage/uploads/<id>/ are removed when older than RetentionHours and
// nothing has touched them. 0 in either field disables the sweep — keeps
// orphans around forever, useful for debugging.
type TusConfig struct {
	RetentionHours int      `json:"retention_hours"`
	SweepInterval  Duration `json:"sweep_interval"`
}

// FTPConfig controls the explicit-FTPS server. TLS is mandatory (only AUTH TLS
// — plain FTP is never accepted); reuses the same cert/key as the web server.
type FTPConfig struct {
	Enabled    bool   `json:"enabled"`
	ListenAddr string `json:"listen_addr"`
	// PublicHost is the IP/hostname returned in PASV replies. Required when
	// the server is reachable through a NAT/firewall (operator must point it
	// to the externally reachable address); leave empty for direct LAN access.
	PublicHost string `json:"public_host,omitempty"`
	// PassivePortMin/Max defines the inclusive range of ports the server uses
	// for PASV data channels. Operators must open this range in the firewall.
	PassivePortMin int `json:"passive_port_min"`
	PassivePortMax int `json:"passive_port_max"`
	// IdleTimeoutSec disconnects clients that idle for longer than this.
	IdleTimeoutSec int `json:"idle_timeout_sec"`
	// Reuses Web.TLS when empty; otherwise allows a separate cert for FTPS.
	TLS TLSConfig `json:"tls"`
}

type TrashConfig struct {
	// RetentionDays is how long a trashed entry survives before the GC worker
	// purges it permanently. 0 disables time-based GC (entries stay until
	// explicit purge via API).
	RetentionDays int `json:"retention_days"`
	// GCInterval is how often the GC worker scans the trash directory.
	GCInterval Duration `json:"gc_interval"`
}

type DatabaseConfig struct {
	DSN string `json:"dsn"`
}

type BackupConfig struct {
	Interval Duration `json:"interval"`
	// Retention is the number of pg_dump archives to keep.
	Retention int `json:"retention"`
	// PGDumpCmd is the argv prefix used to invoke pg_dump. Default ["pg_dump"]
	// assumes the binary is on PATH. For dev setups where PostgreSQL lives in
	// a container, override e.g. ["docker", "exec", "-i", "storman-pg", "pg_dump"].
	PGDumpCmd []string `json:"pg_dump_cmd,omitempty"`
	// PGRestoreCmd mirrors PGDumpCmd for pg_restore. Default ["pg_restore"].
	PGRestoreCmd []string `json:"pg_restore_cmd,omitempty"`
}

type WebConfig struct {
	ListenAddr string    `json:"listen_addr"`
	TLS        TLSConfig `json:"tls"`
	// SecureCookies controls the Secure flag on session/CSRF cookies. Default
	// true; auto-disable here only for local development over plain HTTP.
	SecureCookies bool `json:"secure_cookies"`
	// TrustProxyHeaders enables honouring X-Forwarded-Proto when deciding
	// whether a request is HTTPS (for the cookie Secure flag). Only safe to
	// enable when the server is reachable exclusively through a reverse proxy
	// that strips client-supplied X-Forwarded-* headers — otherwise an
	// attacker can spoof the protocol. Default false.
	TrustProxyHeaders bool `json:"trust_proxy_headers"`
}

type TLSConfig struct {
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
}

// TLSEnabled reports whether both cert and key paths are configured.
func (t TLSConfig) Enabled() bool {
	return t.CertFile != "" && t.KeyFile != ""
}

// Duration is a JSON-friendly time.Duration ("24h", "30m").
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// Default returns a Config skeleton — DSN is a placeholder the operator must edit.
func Default(dataDir string) Config {
	return Config{
		DataDir: dataDir,
		Database: DatabaseConfig{
			DSN: "postgres://storman:storman@localhost:5432/storman?sslmode=disable",
		},
		Backup: BackupConfig{
			Interval:  Duration(24 * time.Hour),
			Retention: 7,
		},
		Web: WebConfig{
			ListenAddr:    ":8443",
			SecureCookies: true,
		},
		Trash: TrashConfig{
			RetentionDays: 30,
			GCInterval:    Duration(1 * time.Hour),
		},
		FTP: FTPConfig{
			Enabled:        false,
			ListenAddr:     ":2121",
			PassivePortMin: 50000,
			PassivePortMax: 50050,
			IdleTimeoutSec: 300,
		},
		Indexing: IndexingConfig{
			Workers:      2,
			PollInterval: Duration(5 * time.Second),
		},
		Tus: TusConfig{
			RetentionHours: 24,
			SweepInterval:  Duration(1 * time.Hour),
		},
		WebDAV: WebDAVConfig{
			Enabled:    false,
			PathPrefix: "/dav",
		},
	}
}

// Load reads config.json from the given path and validates it.
func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.validate(path); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Save writes cfg to path with 0600 perms.
func Save(path string, cfg Config) error {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o600)
}

func (c Config) validate(path string) error {
	if c.DataDir == "" {
		return errors.New("data_dir is empty")
	}
	expectedDir, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("resolve config dir: %w", err)
	}
	configuredDir, err := filepath.Abs(c.DataDir)
	if err != nil {
		return fmt.Errorf("resolve data_dir: %w", err)
	}
	if expectedDir != configuredDir {
		return fmt.Errorf("data_dir %q does not match config location %q (config was likely copied — fix data_dir)", configuredDir, expectedDir)
	}
	if c.Database.DSN == "" {
		return errors.New("database.dsn is empty")
	}
	if _, err := url.Parse(c.Database.DSN); err != nil {
		return fmt.Errorf("database.dsn parse: %w", err)
	}
	key, err := base64.StdEncoding.DecodeString(c.SecretsKey)
	if err != nil {
		return fmt.Errorf("secrets_key not valid base64: %w", err)
	}
	if len(key) != SecretsKeyLen {
		return fmt.Errorf("secrets_key must decode to %d bytes, got %d", SecretsKeyLen, len(key))
	}
	if c.Backup.Retention < 0 {
		return errors.New("backup.retention must be >= 0")
	}
	if time.Duration(c.Backup.Interval) < 0 {
		return errors.New("backup.interval must be >= 0")
	}
	if c.Trash.RetentionDays < 0 {
		return errors.New("trash.retention_days must be >= 0")
	}
	if time.Duration(c.Trash.GCInterval) < 0 {
		return errors.New("trash.gc_interval must be >= 0")
	}
	if c.FTP.Enabled {
		if c.FTP.ListenAddr == "" {
			return errors.New("ftp.listen_addr is empty")
		}
		if c.FTP.PassivePortMin <= 0 || c.FTP.PassivePortMax < c.FTP.PassivePortMin {
			return errors.New("ftp.passive_port_min/max invalid")
		}
	}
	if p := c.WebDAV.PathPrefix; p != "" {
		if p[0] != '/' {
			return fmt.Errorf("webdav.path_prefix must start with '/': %q", p)
		}
		if len(p) > 1 && p[len(p)-1] == '/' {
			return fmt.Errorf("webdav.path_prefix must not end with '/': %q", p)
		}
	}
	return nil
}

// EffectiveTLS returns the FTPS-specific TLS pair if non-empty, otherwise the
// web TLS pair. FTPS is mandatory-TLS, so callers must check Enabled() on the
// result before opening listeners.
func (c Config) EffectiveFTPTLS() TLSConfig {
	if c.FTP.TLS.Enabled() {
		return c.FTP.TLS
	}
	return c.Web.TLS
}
