// Package metrics exposes Prometheus collectors for storman: HTTP request
// counters/histograms, DB pool stats, and outbox depth. The Registry is
// private to keep the default global registry uncluttered.
package metrics

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics bundles the Prometheus registry and the HTTP collectors. One
// instance is created in serve.go and shared with the web layer.
type Metrics struct {
	registry     *prometheus.Registry
	httpRequests *prometheus.CounterVec
	httpDuration *prometheus.HistogramVec
}

// New constructs a fresh registry pre-loaded with the runtime/process
// collectors and the storman HTTP collectors.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		registry: reg,
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "storman_http_requests_total",
			Help: "HTTP requests received, by method, route group, and status.",
		}, []string{"method", "route", "status"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "storman_http_request_duration_seconds",
			Help:    "HTTP request latency by method and route group.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"}),
	}
	reg.MustRegister(m.httpRequests, m.httpDuration)
	return m
}

// Handler returns the /metrics HTTP handler.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// RegisterPoolStats wires gauges that read live values from pool.Stat() on
// each scrape. Cheap — Stat() is in-memory.
func (m *Metrics) RegisterPoolStats(pool *pgxpool.Pool) {
	m.registry.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "storman_db_pool_acquired_conns",
			Help: "Currently acquired (in-use) connections in the pgx pool.",
		},
		func() float64 { return float64(pool.Stat().AcquiredConns()) },
	))
	m.registry.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "storman_db_pool_idle_conns",
			Help: "Currently idle connections in the pgx pool.",
		},
		func() float64 { return float64(pool.Stat().IdleConns()) },
	))
	m.registry.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "storman_db_pool_total_conns",
			Help: "Currently open connections (acquired + idle) in the pgx pool.",
		},
		func() float64 { return float64(pool.Stat().TotalConns()) },
	))
}

// RegisterOutboxDepth reads SELECT COUNT(*) FROM outbox WHERE status=$1 on
// each scrape, once per status. Pending operations are the operational
// signal that crash recovery is needed; the in-flight count is informational.
//
// Cost per scrape: 2 tiny indexed queries. Acceptable for the personal-scale
// deployment.
func (m *Metrics) RegisterOutboxDepth(pool *pgxpool.Pool) {
	for _, status := range []string{"pending", "in_progress"} {
		s := status
		m.registry.MustRegister(prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Name:        "storman_outbox_depth",
				Help:        "Rows in the outbox table by status.",
				ConstLabels: prometheus.Labels{"status": s},
			},
			func() float64 {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				var n int64
				if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE status = $1`, s).Scan(&n); err != nil {
					return -1
				}
				return float64(n)
			},
		))
	}
}

// Wrap returns an http.Handler that records request latency and count for
// every request that flows through it. Self-scrape (/metrics) is skipped.
func (m *Metrics) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		elapsed := time.Since(start).Seconds()
		route := routeGroup(r.URL.Path)
		m.httpRequests.WithLabelValues(r.Method, route, strconv.Itoa(rec.status)).Inc()
		m.httpDuration.WithLabelValues(r.Method, route).Observe(elapsed)
	})
}

// routeGroup collapses the URL path to a low-cardinality label so per-path
// metrics don't explode the time series. Examples:
//
//	/api/fs/stat       -> /api/fs
//	/api/auth/login    -> /api/auth
//	/dav/foo/bar/baz   -> /dav
//	/share/abc/info    -> /share
//	/healthz           -> /healthz
//	/                  -> /
func routeGroup(p string) string {
	if p == "" || p == "/" {
		return "/"
	}
	parts := strings.SplitN(strings.TrimPrefix(p, "/"), "/", 3)
	if len(parts) >= 2 && parts[0] == "api" {
		return "/api/" + parts[1]
	}
	return "/" + parts[0]
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (sr *statusRecorder) WriteHeader(s int) {
	sr.status = s
	sr.ResponseWriter.WriteHeader(s)
}
