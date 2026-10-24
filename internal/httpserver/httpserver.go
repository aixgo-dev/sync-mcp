// Package httpserver serves an MCP server over the Streamable HTTP transport
// with bearer-token authentication and the hardening a network-facing service
// needs: timeouts, size limits, Host and Origin validation, a concurrency cap
// and graceful shutdown.
//
// The server speaks plain HTTP. Terminate TLS in front of it (a reverse proxy,
// ingress or tunnel) and keep it on a private network or loopback.
package httpserver

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	// MCPPath is the path the MCP endpoint is served on.
	MCPPath = "/mcp"
	// HealthPath is the unauthenticated liveness endpoint.
	HealthPath = "/healthz"

	// MinTokenLength is the shortest bearer token accepted. A random token of
	// this length (128 bits as hex) cannot be guessed over the network.
	MinTokenLength = 32

	// MaxBodyBytes is the largest request body accepted. A 256 KiB tool input
	// can grow several times when a client escapes every character, so this
	// leaves room while still bounding what one request can make the server
	// read.
	MaxBodyBytes = 4 << 20

	// MaxInFlight is the number of authenticated MCP requests handled at once.
	// Further requests get 503 so that a misbehaving client cannot turn the
	// server into an unbounded load generator against Sync.
	MaxInFlight = 64

	maxHeaderBytes    = 16 << 10
	readHeaderTimeout = 10 * time.Second
	readTimeout       = 30 * time.Second
	idleTimeout       = 120 * time.Second
	shutdownTimeout   = 15 * time.Second
	failureLogEvery   = 10 * time.Second
)

// Config configures the HTTP transport.
type Config struct {
	// Addr is the listen address, host:port.
	Addr string
	// AuthToken is the bearer token clients must present. Required unless
	// InsecureNoAuth is set.
	AuthToken string
	// InsecureNoAuth disables authentication. It is only accepted when Addr is
	// a loopback address.
	InsecureNoAuth bool
	// AllowedHosts lists the host names (without port) that may appear in the
	// Host header. When empty, only the SDK's built-in protection applies: a
	// request arriving on a loopback socket must carry a loopback Host.
	AllowedHosts []string
	// AllowedOrigins lists the exact origins (scheme://host[:port]) allowed in
	// the Origin header. Requests that carry an Origin header not in this list
	// are rejected; requests without one (non-browser clients) are unaffected.
	AllowedOrigins []string
	// UpstreamTimeout is the per-request timeout for calls to Sync. It is used
	// to size the server's write timeout.
	UpstreamTimeout time.Duration
	// Logger receives operational messages. It never receives the token. May be nil.
	Logger *log.Logger
}

// IsLoopbackAddr reports whether a host:port listen address binds only the
// loopback interface. An empty host (all interfaces) is not loopback.
func IsLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	return host == "localhost" || net.ParseIP(host).IsLoopback()
}

// Validate checks the configuration and returns the first problem found.
func (c Config) Validate() error {
	if _, _, err := net.SplitHostPort(c.Addr); err != nil {
		return fmt.Errorf("invalid listen address %q: %v (expected host:port)", c.Addr, err)
	}
	token := strings.TrimSpace(c.AuthToken)
	switch {
	case c.InsecureNoAuth && token != "":
		return errors.New("--insecure-no-auth cannot be combined with an auth token")
	case c.InsecureNoAuth:
		if !IsLoopbackAddr(c.Addr) {
			return fmt.Errorf("--insecure-no-auth is only allowed on a loopback listen address (127.0.0.1, [::1] or localhost), not %q", c.Addr)
		}
	case token == "":
		return errors.New("the http transport requires a bearer token: set SYNC_MCP_AUTH_TOKEN (or --auth-token). " +
			"To run without authentication for local development only, pass --insecure-no-auth with a loopback --listen address")
	default:
		if len(token) < MinTokenLength {
			return fmt.Errorf("the auth token must be at least %d characters; generate one with: openssl rand -hex 32", MinTokenLength)
		}
		for i := 0; i < len(token); i++ {
			if b := token[i]; b <= ' ' || b > '~' {
				return errors.New("the auth token must contain only printable ASCII characters without spaces")
			}
		}
	}
	for _, h := range c.AllowedHosts {
		bare := strings.Trim(h, "[]") // an IPv6 literal may be written in brackets
		invalid := bare == "" || strings.ContainsAny(bare, "/*@ ") || (strings.Contains(bare, ":") && net.ParseIP(bare) == nil)
		if invalid {
			return fmt.Errorf("invalid allowed host %q: give a bare host name such as sync-mcp.example.com, without scheme, port or wildcard", h)
		}
	}
	for _, o := range c.AllowedOrigins {
		u, err := url.Parse(o)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.User != nil {
			return fmt.Errorf("invalid allowed origin %q: expected scheme://host[:port], for example https://app.example.com", o)
		}
	}
	return nil
}

// New builds an http.Server that serves mcpServer. The config must be valid.
func New(mcpServer *mcp.Server, cfg Config) (*http.Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	hosts := map[string]bool{}
	for _, h := range cfg.AllowedHosts {
		hosts[normalizeHost(h)] = true
	}
	origins := map[string]bool{}
	for _, o := range cfg.AllowedOrigins {
		origins[normalizeOrigin(o)] = true
	}

	opts := &mcp.StreamableHTTPOptions{
		// Every tool is a plain request/response call and the server never
		// sends requests to the client, so no per-client session state is
		// kept. That also means a restart or a rolling deploy never strands a
		// client with an unknown session id, and there is no per-session
		// memory for a client to grow.
		Stateless: true,
		// Plain JSON responses instead of SSE streams proxy cleanly through
		// buffering reverse proxies and tunnels.
		JSONResponse:        true,
		MaxRequestBodyBytes: MaxBodyBytes,
		// With an explicit host allowlist our own check replaces the SDK's
		// loopback-only one, which would otherwise reject the public host
		// name when a tunnel connects over loopback.
		DisableLocalhostProtection: len(hosts) > 0,
	}
	mcpHandler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return mcpServer }, opts)

	var h http.Handler = http.MaxBytesHandler(mcpHandler, MaxBodyBytes)
	h = limitInFlight(h, MaxInFlight)
	if !cfg.InsecureNoAuth {
		h = newAuthenticator(cfg.AuthToken, cfg.Logger).wrap(h)
	}
	h = checkHostAndOrigin(h, hosts, origins)

	mux := http.NewServeMux()
	mux.HandleFunc("GET "+HealthPath, healthz)
	mux.Handle(MCPPath, h)
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "not found", http.StatusNotFound) })

	upstream := cfg.UpstreamTimeout
	if upstream <= 0 {
		upstream = 15 * time.Second
	}
	return &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		// A list call can make two requests to Sync; leave room for both.
		WriteTimeout:   2*upstream + 15*time.Second,
		IdleTimeout:    idleTimeout,
		MaxHeaderBytes: maxHeaderBytes,
		ErrorLog:       cfg.Logger,
	}, nil
}

// Serve serves on ln until ctx is cancelled, then shuts down gracefully,
// giving in-flight requests a few seconds to finish.
func Serve(ctx context.Context, srv *http.Server, ln net.Listener) error {
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}
	shutCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		_ = srv.Close()
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	if err := <-errc; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// healthz is a liveness probe. It reveals nothing: no version, no tenant, no
// Sync status, and it does not call Sync.
func healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte("ok\n"))
}

// authenticator checks bearer tokens.
type authenticator struct {
	want     [sha256.Size]byte
	logger   *log.Logger
	failures atomic.Uint64
	lastLog  atomic.Int64
}

func newAuthenticator(token string, logger *log.Logger) *authenticator {
	return &authenticator{want: sha256.Sum256([]byte(strings.TrimSpace(token))), logger: logger}
}

func (a *authenticator) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, present := bearerToken(r.Header.Get("Authorization"))
		// Compare fixed-length digests in constant time, so neither the
		// content nor the length of the presented token affects timing. The
		// comparison runs even when no token was presented.
		have := sha256.Sum256([]byte(got))
		ok := subtle.ConstantTimeCompare(have[:], a.want[:]) == 1
		if !ok || !present {
			a.noteFailure(r)
			w.Header().Set("WWW-Authenticate", `Bearer realm="sync-mcp"`)
			w.Header().Set("Cache-Control", "no-store")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// noteFailure logs authentication failures at most once per interval, with a
// running count and the peer address, never any request-supplied text.
func (a *authenticator) noteFailure(r *http.Request) {
	n := a.failures.Add(1)
	if a.logger == nil {
		return
	}
	now := time.Now().UnixNano()
	last := a.lastLog.Load()
	if now-last < int64(failureLogEvery) || !a.lastLog.CompareAndSwap(last, now) {
		return
	}
	peer, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		peer = "unknown"
	}
	a.logger.Printf("rejected unauthenticated request(s): %d so far, latest from %s", n, peer)
}

// bearerToken extracts the token from an Authorization header value.
func bearerToken(header string) (string, bool) {
	scheme, token, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	token = strings.TrimSpace(token)
	return token, token != ""
}

// limitInFlight rejects requests beyond n concurrent ones with 503.
func limitInFlight(next http.Handler, n int) http.Handler {
	sem := make(chan struct{}, n)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
			next.ServeHTTP(w, r)
		default:
			w.Header().Set("Retry-After", "1")
			http.Error(w, "too many concurrent requests", http.StatusServiceUnavailable)
		}
	})
}

// checkHostAndOrigin rejects requests with an unexpected Host or Origin header
// with 403, as the MCP specification asks of Streamable HTTP servers to defend
// against DNS rebinding and cross-site requests.
func checkHostAndOrigin(next http.Handler, hosts, origins map[string]bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(hosts) > 0 && !hosts[normalizeHost(hostOnly(r.Host))] {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		if vals := r.Header.Values("Origin"); len(vals) > 0 {
			if len(vals) > 1 || !origins[normalizeOrigin(vals[0])] {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// hostOnly strips an optional port from a Host header value.
func hostOnly(hostport string) string {
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		return h
	}
	return strings.Trim(hostport, "[]")
}

func normalizeHost(h string) string {
	return strings.TrimSuffix(strings.ToLower(strings.Trim(h, "[]")), ".")
}

func normalizeOrigin(o string) string {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(o)), "/")
}
