package ftpsrv

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	ftpserver "github.com/fclairamb/ftpserverlib"

	"github.com/alexnav/storman/internal/auth"
	"github.com/alexnav/storman/internal/rbac"
	"github.com/alexnav/storman/internal/storage/dbfs"
)

// authTimeout bounds the auth call (argon2 verify + DB roundtrip). Generous
// because argon2 is intentionally slow.
const authTimeout = 30 * time.Second

// Config is the runtime tuning for the FTPS server.
type Config struct {
	ListenAddr     string
	PublicHost     string
	PassivePortMin int
	PassivePortMax int
	IdleTimeoutSec int
	// TLSConfig must be non-nil — plain FTP is rejected per docs/arch/interfaces.md.
	TLSConfig *tls.Config
	// Limiter rate-limits AuthUser by (login, client IP). Should be the same
	// *auth.LoginLimiter the HTTP/WebDAV surfaces use, so an attacker can't
	// bypass the limit by switching protocols. Nil disables limiting.
	Limiter *auth.LoginLimiter
}

// Driver implements ftpserver.MainDriver. One Driver instance is shared
// across all connections; AuthUser hands back a fresh per-client clientFS.
type Driver struct {
	cfg    Config
	users  *auth.UserService
	perms  *rbac.PermissionService
	fs     *dbfs.DBFS
	logger *log.Logger
}

// NewDriver wires the driver. The provided services are read-only from the
// driver's perspective; nothing about them is rebuilt per-connection.
func NewDriver(cfg Config, users *auth.UserService, perms *rbac.PermissionService, fs *dbfs.DBFS, logger *log.Logger) *Driver {
	if logger == nil {
		logger = log.Default()
	}
	return &Driver{cfg: cfg, users: users, perms: perms, fs: fs, logger: logger}
}

// GetSettings returns the ftpserver settings. TLS is mandatory.
func (d *Driver) GetSettings() (*ftpserver.Settings, error) {
	if d.cfg.TLSConfig == nil {
		return nil, errors.New("ftpsrv: TLS config is required — refusing to expose plain FTP")
	}
	return &ftpserver.Settings{
		ListenAddr:               d.cfg.ListenAddr,
		PublicHost:               d.cfg.PublicHost,
		PassiveTransferPortRange: ftpserver.PortRange{Start: d.cfg.PassivePortMin, End: d.cfg.PassivePortMax},
		IdleTimeout:              d.cfg.IdleTimeoutSec,
		TLSRequired:              ftpserver.MandatoryEncryption,
		Banner:                   "storman ftps",
		DisableSite:              true,
		// Don't accept IP mismatch — keeps data channels tied to control IP.
		ActiveConnectionsCheck: ftpserver.IPMatchRequired,
		PasvConnectionsCheck:   ftpserver.IPMatchRequired,
	}, nil
}

// ClientConnected is called on each new control-channel connection.
func (d *Driver) ClientConnected(cc ftpserver.ClientContext) (string, error) {
	d.logger.Printf("ftp: client %d connected from %s", cc.ID(), cc.RemoteAddr())
	return "storman FTPS — login with your storman credentials or an app password", nil
}

// ClientDisconnected fires once the connection is torn down.
func (d *Driver) ClientDisconnected(cc ftpserver.ClientContext) {
	d.logger.Printf("ftp: client %d disconnected", cc.ID())
}

// AuthUser authenticates the user and returns the per-client ClientDriver.
// Accepts the main password or any of the user's app_passwords.
func (d *Driver) AuthUser(cc ftpserver.ClientContext, login, pass string) (ftpserver.ClientDriver, error) {
	// Mandatory-encryption is enforced via Settings.TLSRequired; double-check
	// here so a misconfigured server can't slip a plain auth through.
	if !cc.HasTLSForControl() {
		return nil, errors.New("ftpsrv: TLS required on control channel")
	}
	// Rate-limit before the (expensive) argon2 verify. Same limiter as the
	// HTTP/WebDAV surfaces, so brute-force attempts cross-protocol are
	// counted together.
	ip := ""
	if addr := cc.RemoteAddr(); addr != nil {
		if host, _, splitErr := net.SplitHostPort(addr.String()); splitErr == nil {
			ip = host
		}
	}
	if ok, _ := d.cfg.Limiter.Allow(login, ip); !ok {
		d.logger.Printf("ftp: rate-limited auth for %q from %s (client %d)", login, ip, cc.ID())
		return nil, fmt.Errorf("login failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), authTimeout)
	defer cancel()
	user, err := d.users.AuthenticateAppPassword(ctx, login, pass)
	if err != nil {
		if errors.Is(err, auth.ErrPasswordMismatch) ||
			errors.Is(err, auth.ErrNotFound) ||
			errors.Is(err, auth.ErrAccountLocked) {
			return nil, fmt.Errorf("login failed")
		}
		d.logger.Printf("ftp: auth backend error for %q: %v", login, err)
		return nil, fmt.Errorf("login failed")
	}
	d.logger.Printf("ftp: user %s authenticated (client %d)", user.Login, cc.ID())
	return &clientFS{user: &user, fs: d.fs, perms: d.perms}, nil
}

// GetTLSConfig returns the TLS config for AUTH TLS / data channels.
func (d *Driver) GetTLSConfig() (*tls.Config, error) {
	if d.cfg.TLSConfig == nil {
		return nil, errors.New("ftpsrv: no TLS configured")
	}
	return d.cfg.TLSConfig, nil
}

// WrapPassiveListener is the optional MainDriverExtensionPassiveWrapper hook
// — we pass it through unchanged. Reserved for future logging/throttling.
func (d *Driver) WrapPassiveListener(listener net.Listener) (net.Listener, error) {
	return listener, nil
}
