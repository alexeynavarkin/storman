package cli

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/alexnav/storman/internal/audit"
	"github.com/alexnav/storman/internal/auth"
	wauth "github.com/alexnav/storman/internal/auth/webauthn"
	"github.com/alexnav/storman/internal/backup"
	"github.com/alexnav/storman/internal/config"
	"github.com/alexnav/storman/internal/datadir"
	"github.com/alexnav/storman/internal/db"
	"github.com/alexnav/storman/internal/ftpsrv"
	"github.com/alexnav/storman/internal/jobs"
	"github.com/alexnav/storman/internal/metrics"
	"github.com/alexnav/storman/internal/rbac"
	"github.com/alexnav/storman/internal/storage"
	"github.com/alexnav/storman/internal/storage/dbfs"
	"github.com/alexnav/storman/internal/storage/flat"
	"github.com/alexnav/storman/internal/web"
)

func newServeCmd(configPath *string) *cobra.Command {
	var uiDir string
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the storman HTTP server",
		RunE: func(cmd *cobra.Command, args []string) error {
			resolvedPath := resolveConfigPath(*configPath)
			cfg, err := config.Load(resolvedPath)
			if err != nil {
				return err
			}
			return runServe(cmd.Context(), cfg, resolvedPath, uiDir)
		},
	}
	cmd.Flags().StringVar(&uiDir, "ui-dir", "", "directory containing the built SPA (omit for API-only mode; use Vite dev server during development)")
	return cmd
}

func runServe(ctx context.Context, cfg config.Config, configPath, uiDir string) error {
	pool, err := db.NewPool(ctx, cfg.Database.DSN)
	if err != nil {
		return err
	}
	defer pool.Close()

	flatBackend := flat.New(datadir.StorageDir(cfg.DataDir), datadir.UploadsDir(cfg.DataDir))
	fs := dbfs.New(pool, datadir.TrashDir(cfg.DataDir), flatBackend)
	if _, err := fs.Bootstrap(ctx); err != nil {
		return fmt.Errorf("bootstrap root: %w", err)
	}
	if acted, err := fs.RecoverPending(ctx); err != nil {
		return fmt.Errorf("recover pending: %w", err)
	} else if acted > 0 {
		slog.Info("startup: recovered pending outbox rows", "count", acted)
	}
	fs.StartGC(ctx, time.Duration(cfg.Trash.GCInterval), cfg.Trash.RetentionDays, nil)

	// Async indexing pipeline: one worker pool per kind. Hashes are the only
	// kind in MVP; adding EXIF / face workers is a second NewPool call here.
	jobSvc := jobs.NewService(pool)
	hasher := jobs.NewHasher(pool, map[string]storage.FileBackend{flatBackend.Name(): flatBackend})
	jobsCtx, stopJobs := context.WithCancel(ctx)
	hashPool := jobs.NewPool(jobSvc, jobs.KindHash, hasher, cfg.Indexing.Workers, time.Duration(cfg.Indexing.PollInterval), nil)
	jobsWait := hashPool.Start(jobsCtx)
	defer func() { stopJobs(); jobsWait() }()

	backup.Start(ctx,
		time.Duration(cfg.Backup.Interval),
		cfg.Backup.Retention,
		cfg.DataDir,
		cfg.Database.DSN,
		backup.Tools{DumpCmd: cfg.Backup.PGDumpCmd, RestoreCmd: cfg.Backup.PGRestoreCmd},
		nil,
	)

	users := auth.NewUserService(pool)
	sessions := auth.NewSessionService(pool)
	perms := rbac.NewPermissionService(pool)
	shares := auth.NewShareLinkService(pool)
	auditSvc := audit.NewService(pool, nil)
	// One shared limiter across HTTP login, WebDAV Basic auth, and FTPS auth
	// — switching protocols can't bypass the rate limit.
	loginLimiter := auth.NewLoginLimiter()

	// Prometheus registry: HTTP middleware + DB pool gauges + outbox depth.
	met := metrics.New()
	met.RegisterPoolStats(pool)
	met.RegisterOutboxDepth(pool)

	tlsEnabled := cfg.Web.TLS.Enabled()
	if cfg.Web.SecureCookies && !tlsEnabled && !cfg.Web.TrustProxyHeaders {
		slog.Warn("secure_cookies requested but neither TLS nor trust_proxy_headers is configured — Secure flag will only be set on requests arriving over HTTPS")
	}

	server := web.NewServer(web.Config{
		SecureCookies:     cfg.Web.SecureCookies,
		LocalTLS:          tlsEnabled,
		TrustProxyHeaders: cfg.Web.TrustProxyHeaders,
		LoginLimiter:      loginLimiter,
		Ready:             func(ctx context.Context) error { return pool.Ping(ctx) },
		MetricsHandler:    met.Handler(),
		ConfigPath:        configPath,
	}, users, sessions, perms, shares, fs, auditSvc)
	if cfg.WebAuthn.Enabled() {
		passkeys, err := wauth.NewService(pool, wauth.Config{
			RPID:          cfg.WebAuthn.RPID,
			RPDisplayName: cfg.WebAuthn.RPDisplayName,
			RPOrigins:     cfg.WebAuthn.RPOrigins,
			ChallengeTTL:  time.Duration(cfg.WebAuthn.ChallengeTTLSec) * time.Second,
		})
		if err != nil {
			return fmt.Errorf("webauthn: %w", err)
		}
		server.Passkeys = passkeys
		sweepInterval := time.Duration(cfg.WebAuthn.SweepIntervalSec) * time.Second
		if sweepInterval <= 0 {
			sweepInterval = time.Minute
		}
		server.StartWebAuthnSweeper(ctx, sweepInterval, nil)
		slog.Info("passkey auth enabled", "rp_id", cfg.WebAuthn.RPID, "origins", cfg.WebAuthn.RPOrigins)
	}
	if err := server.InitSetup(ctx); err != nil {
		return err
	}
	server.StartTusSweeper(ctx, cfg.Tus.RetentionHours, time.Duration(cfg.Tus.SweepInterval), nil)
	if cfg.WebDAV.Enabled {
		prefix := cfg.WebDAV.PathPrefix
		if prefix == "" {
			prefix = "/dav"
		}
		server = server.SetDav(true, prefix)
		if !tlsEnabled {
			slog.Warn("webdav enabled but TLS is not configured — basic credentials will travel in clear")
		}
		slog.Info("webdav mounted", "prefix", prefix)
	}
	switch {
	case uiDir != "":
		spa, err := web.SPAFromDir(uiDir)
		if err != nil {
			return fmt.Errorf("ui-dir: %w", err)
		}
		server = server.WithSPA(spa)
		slog.Info("serving SPA from directory", "dir", uiDir)
	default:
		if assets, ok := web.EmbeddedSPA(); ok {
			server = server.WithSPA(web.SPAFromFS(assets))
			slog.Info("serving SPA from the embedded build")
		} else {
			slog.Info("running in API-only mode (no --ui-dir and no embedded SPA) — use Vite dev server for UI")
		}
	}
	httpSrv := &http.Server{
		Addr:              cfg.Web.ListenAddr,
		Handler:           met.Wrap(server.Handler),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	if tlsEnabled {
		httpSrv.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 2)
	go func() {
		if tlsEnabled {
			slog.Info("HTTP listener up", "scheme", "https", "addr", cfg.Web.ListenAddr)
			errCh <- httpSrv.ListenAndServeTLS(cfg.Web.TLS.CertFile, cfg.Web.TLS.KeyFile)
		} else {
			slog.Warn("HTTP listener up without TLS — development only", "scheme", "http", "addr", cfg.Web.ListenAddr)
			errCh <- httpSrv.ListenAndServe()
		}
	}()

	// Optional FTPS server. Requires TLS — falls back to a warning and stays
	// off if no cert is configured anywhere.
	var ftpServer *ftpsrv.Server
	if cfg.FTP.Enabled {
		ftpTLS := cfg.EffectiveFTPTLS()
		if !ftpTLS.Enabled() {
			slog.Warn("ftp.enabled=true but no TLS cert/key configured — FTPS server not started")
		} else {
			tlsCfg, err := loadTLSConfig(ftpTLS)
			if err != nil {
				return fmt.Errorf("ftp tls: %w", err)
			}
			driver := ftpsrv.NewDriver(ftpsrv.Config{
				ListenAddr:     cfg.FTP.ListenAddr,
				PublicHost:     cfg.FTP.PublicHost,
				PassivePortMin: cfg.FTP.PassivePortMin,
				PassivePortMax: cfg.FTP.PassivePortMax,
				IdleTimeoutSec: cfg.FTP.IdleTimeoutSec,
				TLSConfig:      tlsCfg,
				Limiter:        loginLimiter,
			}, users, perms, fs, nil)
			ftpServer = ftpsrv.NewServer(driver, nil)
			go func() {
				slog.Info("FTPS listener up",
					"addr", cfg.FTP.ListenAddr,
					"passive_min", cfg.FTP.PassivePortMin,
					"passive_max", cfg.FTP.PassivePortMax)
				errCh <- ftpServer.Serve(ctx)
			}()
		}
	}

	select {
	case <-ctx.Done():
		slog.Info("shutdown signal received")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("graceful shutdown: %w", err)
		}
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// loadTLSConfig builds a TLS config from cert/key paths. Used for the FTPS
// listener; the HTTP server uses net/http's own ListenAndServeTLS path.
func loadTLSConfig(t config.TLSConfig) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}, nil
}
