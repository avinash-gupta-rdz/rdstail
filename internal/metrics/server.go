package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server exposes /metrics, /healthz, /readyz.
type Server struct {
	addr   string
	srv    *http.Server
	log    *slog.Logger
	ready  atomic.Bool
}

// NewServer wires an http.Server over the given Metrics registry.
func NewServer(addr string, m *Metrics, lg *slog.Logger) *Server {
	if lg == nil {
		lg = slog.Default()
	}
	mux := http.NewServeMux()
	s := &Server{addr: addr, log: lg}
	mux.Handle("/metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if s.ready.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready\n"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("starting\n"))
	})
	s.srv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

// MarkReady flips /readyz to 200.
func (s *Server) MarkReady() { s.ready.Store(true) }

// Run starts the server and blocks until ctx is cancelled, at which point it
// performs a graceful shutdown.
func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		s.log.Info("metrics server listening", "addr", s.addr)
		if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.srv.Shutdown(shutdownCtx); err != nil {
			s.log.Warn("metrics server shutdown", "err", err)
		}
		return nil
	}
}
