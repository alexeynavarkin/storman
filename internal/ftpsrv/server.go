package ftpsrv

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"

	ftpserver "github.com/fclairamb/ftpserverlib"
)

// Server wraps an ftpserver.FtpServer for graceful start/stop. Callers
// construct it via NewServer, then run Serve in a goroutine and Stop when
// ctx is cancelled.
type Server struct {
	srv     *ftpserver.FtpServer
	logger  *log.Logger
	stopped chan struct{}
}

// NewServer assembles the FTPS server. The driver is the same Driver value
// used for every connection.
func NewServer(driver *Driver, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.Default()
	}
	srv := ftpserver.NewFtpServer(driver)
	// Quiet ftpserverlib's internal slog stream — our Driver hooks already
	// log connect / disconnect / auth events at the level we care about.
	srv.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	_ = os.Stderr
	return &Server{srv: srv, logger: logger, stopped: make(chan struct{})}
}

// Serve blocks until ListenAndServe returns. Cancelling ctx invokes a
// graceful stop. Returns nil for a clean shutdown.
func (s *Server) Serve(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.srv.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
		_ = s.srv.Stop()
		<-errCh // drain — ListenAndServe returns once the listener closes.
		close(s.stopped)
		return nil
	case err := <-errCh:
		close(s.stopped)
		if err == nil {
			return nil
		}
		return fmt.Errorf("ftpserver: %w", err)
	}
}
