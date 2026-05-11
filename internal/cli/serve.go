package cli

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/alexnav/storman/internal/audit"
	"github.com/alexnav/storman/internal/auth"
	"github.com/alexnav/storman/internal/backup"
	"github.com/alexnav/storman/internal/config"
	"github.com/alexnav/storman/internal/datadir"
	"github.com/alexnav/storman/internal/db"
	"github.com/alexnav/storman/internal/ftpsrv"
	"github.com/alexnav/storman/internal/jobs"
	"github.com/alexnav/storman/internal/rbac"
	"github.com/alexnav/storman/internal/storage"
	"github.com/alexnav/storman/internal/storage/dbfs"
	"github.com/alexnav/storman/internal/storage/flat"
	"github.com/alexnav/storman/internal/web"
)

func newServeCmd() *cobra.Command {
	var (
		dataDir string
		uiDir   string
	)
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the storman HTTP server",
		RunE: func(cmd *cobra.Command, args []string) error {
			if dataDir == "" {
				return fmt.Errorf("--data-dir is required")
			}
			cfg, err := config.Load(datadir.ConfigPath(dataDir))
			if err != nil {
				return err
			}
			return runServe(cmd.Context(), cfg, uiDir)
		},
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", "", "path to the storman data directory")
	cmd.Flags().StringVar(&uiDir, "ui-dir", "", "directory containing the built SPA (omit for API-only mode; use Vite dev server during development)")
	_ = cmd.MarkFlagRequired("data-dir")
	return cmd
}

func runServe(ctx context.Context, cfg config.Config, uiDir string) error {
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
		log.Printf("startup: recovered %d pending outbox row(s)", acted)
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

	tlsEnabled := cfg.Web.TLS.Enabled()
	secureCookies := cfg.Web.SecureCookies && tlsEnabled
	if cfg.Web.SecureCookies && !tlsEnabled {
		log.Printf("warning: secure_cookies requested but TLS is not configured — disabling Secure flag for development")
	}

	server := web.NewServer(web.Config{SecureCookies: secureCookies}, users, sessions, perms, shares, fs, auditSvc)
	server.StartTusSweeper(ctx, cfg.Tus.RetentionHours, time.Duration(cfg.Tus.SweepInterval), nil)
	switch {
	case uiDir != "":
		spa, err := web.SPAFromDir(uiDir)
		if err != nil {
			return fmt.Errorf("ui-dir: %w", err)
		}
		server = server.WithSPA(spa)
		log.Printf("serving SPA from %s", uiDir)
	default:
		if assets, ok := web.EmbeddedSPA(); ok {
			server = server.WithSPA(web.SPAFromFS(assets))
			log.Printf("serving SPA from the embedded build")
		} else {
			log.Printf("running in API-only mode (no --ui-dir and no embedded SPA) — use Vite dev server for UI")
		}
	}
	httpSrv := &http.Server{
		Addr:              cfg.Web.ListenAddr,
		Handler:           server.Handler,
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
			log.Printf("storman serve: HTTPS on %s", cfg.Web.ListenAddr)
			errCh <- httpSrv.ListenAndServeTLS(cfg.Web.TLS.CertFile, cfg.Web.TLS.KeyFile)
		} else {
			log.Printf("storman serve: HTTP on %s (TLS not configured — development only)", cfg.Web.ListenAddr)
			errCh <- httpSrv.ListenAndServe()
		}
	}()

	// Optional FTPS server. Requires TLS — falls back to a warning and stays
	// off if no cert is configured anywhere.
	var ftpServer *ftpsrv.Server
	if cfg.FTP.Enabled {
		ftpTLS := cfg.EffectiveFTPTLS()
		if !ftpTLS.Enabled() {
			log.Printf("warning: ftp.enabled=true but no TLS cert/key configured — FTPS server not started")
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
			}, users, perms, fs, nil)
			ftpServer = ftpsrv.NewServer(driver, nil)
			go func() {
				log.Printf("storman ftps: listening on %s (passive %d-%d)", cfg.FTP.ListenAddr, cfg.FTP.PassivePortMin, cfg.FTP.PassivePortMax)
				errCh <- ftpServer.Serve(ctx)
			}()
		}
	}

	select {
	case <-ctx.Done():
		log.Printf("shutdown signal received")
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
