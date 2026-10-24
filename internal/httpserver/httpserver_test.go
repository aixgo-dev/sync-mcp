package httpserver_test

import (
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/aixgo-dev/sync-mcp/internal/httpserver"
)

const goodToken = "0123456789abcdef0123456789abcdef"

func newMCP() *mcp.Server {
	s := mcp.NewServer(&mcp.Implementation{Name: "t", Version: "0"}, nil)
	type in struct {
		Text string `json:"text"`
	}
	type out struct {
		Echo string `json:"echo"`
	}
	mcp.AddTool(s, &mcp.Tool{Name: "echo", Description: "echo"}, func(_ context.Context, _ *mcp.CallToolRequest, a in) (*mcp.CallToolResult, out, error) {
		return nil, out{Echo: a.Text}, nil
	})
	return s
}

func handler(t *testing.T, cfg httpserver.Config) http.Handler {
	t.Helper()
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:0"
	}
	srv, err := httpserver.New(newMCP(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return srv.Handler
}

func do(h http.Handler, method, path string, hdr map[string]string, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Host = "127.0.0.1:8081"
	for k, v := range hdr {
		if k == "Host" {
			req.Host = v
			continue
		}
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

const initBody = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"c","version":"0"}}}`

func mcpHeaders(extra map[string]string) map[string]string {
	m := map[string]string{"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name    string
		cfg     httpserver.Config
		wantErr string
	}{
		{"no token", httpserver.Config{Addr: "127.0.0.1:8081"}, "requires a bearer token"},
		{"blank token", httpserver.Config{Addr: "127.0.0.1:8081", AuthToken: "   "}, "requires a bearer token"},
		{"short token", httpserver.Config{Addr: "127.0.0.1:8081", AuthToken: "short"}, "at least 16"},
		{"token with space", httpserver.Config{Addr: "127.0.0.1:8081", AuthToken: "aaaaaaaa bbbbbbbbbbbb"}, "printable ASCII"},
		{"good token loopback", httpserver.Config{Addr: "127.0.0.1:8081", AuthToken: goodToken}, ""},
		{"good token all interfaces", httpserver.Config{Addr: ":8081", AuthToken: goodToken}, ""},
		{"insecure loopback v4", httpserver.Config{Addr: "127.0.0.1:8081", InsecureNoAuth: true}, ""},
		{"insecure loopback v6", httpserver.Config{Addr: "[::1]:8081", InsecureNoAuth: true}, ""},
		{"insecure localhost", httpserver.Config{Addr: "localhost:8081", InsecureNoAuth: true}, ""},
		{"insecure all interfaces", httpserver.Config{Addr: ":8081", InsecureNoAuth: true}, "loopback"},
		{"insecure 0.0.0.0", httpserver.Config{Addr: "0.0.0.0:8081", InsecureNoAuth: true}, "loopback"},
		{"insecure ipv6 any", httpserver.Config{Addr: "[::]:8081", InsecureNoAuth: true}, "loopback"},
		{"insecure private ip", httpserver.Config{Addr: "10.0.0.5:8081", InsecureNoAuth: true}, "loopback"},
		{"insecure hostname", httpserver.Config{Addr: "example.com:8081", InsecureNoAuth: true}, "loopback"},
		{"insecure with token", httpserver.Config{Addr: "127.0.0.1:8081", InsecureNoAuth: true, AuthToken: goodToken}, "cannot be combined"},
		{"bad addr", httpserver.Config{Addr: "8081", AuthToken: goodToken}, "invalid listen address"},
		{"bad host entry", httpserver.Config{Addr: ":1", AuthToken: goodToken, AllowedHosts: []string{"https://x.example.com"}}, "invalid allowed host"},
		{"wildcard host", httpserver.Config{Addr: ":1", AuthToken: goodToken, AllowedHosts: []string{"*.example.com"}}, "invalid allowed host"},
		{"host with port", httpserver.Config{Addr: ":1", AuthToken: goodToken, AllowedHosts: []string{"x.example.com:443"}}, "invalid allowed host"},
		{"bad origin", httpserver.Config{Addr: ":1", AuthToken: goodToken, AllowedOrigins: []string{"example.com"}}, "invalid allowed origin"},
		{"origin with path", httpserver.Config{Addr: ":1", AuthToken: goodToken, AllowedOrigins: []string{"https://example.com/app"}}, "invalid allowed origin"},
		{"good lists", httpserver.Config{Addr: ":1", AuthToken: goodToken, AllowedHosts: []string{"x.example.com", "[::1]"}, AllowedOrigins: []string{"https://app.example.com"}}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			switch {
			case tc.wantErr == "" && err != nil:
				t.Errorf("unexpected error: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Errorf("error = %v, want one containing %q", err, tc.wantErr)
			}
			if err != nil && strings.Contains(err.Error(), goodToken) {
				t.Error("error leaks the token")
			}
		})
	}
}

func TestNewRefusesInvalidConfig(t *testing.T) {
	if _, err := httpserver.New(newMCP(), httpserver.Config{Addr: "127.0.0.1:0"}); err == nil {
		t.Fatal("New must refuse to build a server without a token")
	}
}

func TestAuth(t *testing.T) {
	h := handler(t, httpserver.Config{AuthToken: goodToken})
	cases := []struct {
		name   string
		header string
		want   int
	}{
		{"missing", "", 401},
		{"wrong", "Bearer " + strings.Repeat("x", 32), 401},
		{"prefix of right", "Bearer " + goodToken[:20], 401},
		{"right plus suffix", "Bearer " + goodToken + "x", 401},
		{"empty bearer", "Bearer ", 401},
		{"basic scheme", "Basic " + goodToken, 401},
		{"raw token no scheme", goodToken, 401},
		{"right", "Bearer " + goodToken, 200},
		{"right lowercase scheme", "bearer " + goodToken, 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hdr := mcpHeaders(nil)
			if tc.header != "" {
				hdr["Authorization"] = tc.header
			}
			rec := do(h, "POST", "/mcp", hdr, initBody)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d; body %q", rec.Code, tc.want, rec.Body.String())
			}
			if tc.want == 401 {
				if got := rec.Header().Get("WWW-Authenticate"); !strings.HasPrefix(got, "Bearer") {
					t.Errorf("WWW-Authenticate = %q", got)
				}
				if strings.Contains(rec.Body.String(), goodToken) {
					t.Error("401 body leaks the token")
				}
			}
		})
	}
}

func TestInsecureNoAuthServesWithoutToken(t *testing.T) {
	h := handler(t, httpserver.Config{Addr: "127.0.0.1:0", InsecureNoAuth: true})
	if rec := do(h, "POST", "/mcp", mcpHeaders(nil), initBody); rec.Code != 200 {
		t.Fatalf("status = %d; %s", rec.Code, rec.Body.String())
	}
}

func TestHealthz(t *testing.T) {
	h := handler(t, httpserver.Config{AuthToken: goodToken})
	rec := do(h, "GET", "/healthz", nil, "")
	if rec.Code != 200 || strings.TrimSpace(rec.Body.String()) != "ok" {
		t.Fatalf("healthz = %d %q", rec.Code, rec.Body.String())
	}
	// A wrong Host or an Origin does not matter for the probe, and nothing
	// else leaks: no version, no tenant, no token.
	rec = do(h, "GET", "/healthz", map[string]string{"Host": "10.1.2.3:8081", "Origin": "https://evil.example"}, "")
	if rec.Code != 200 || rec.Body.Len() > 8 {
		t.Fatalf("healthz = %d %q", rec.Code, rec.Body.String())
	}
	if rec := do(h, "POST", "/healthz", nil, ""); rec.Code != 404 && rec.Code != 405 {
		t.Errorf("POST /healthz = %d, want 404 or 405", rec.Code)
	}
}

func TestUnknownPathsAreNotFoundWithoutAuth(t *testing.T) {
	h := handler(t, httpserver.Config{AuthToken: goodToken})
	for _, p := range []string{"/", "/admin", "/mcp/", "/.well-known/oauth-protected-resource"} {
		if rec := do(h, "GET", p, nil, ""); rec.Code != 404 {
			t.Errorf("GET %s = %d, want 404", p, rec.Code)
		}
	}
}

func TestHostValidation(t *testing.T) {
	auth := map[string]string{"Authorization": "Bearer " + goodToken}

	// Default on a loopback listener: SDK protection rejects non-loopback Host.
	// httptest requests have no loopback local address, so exercise the
	// allowlist, which is what a tunnel deployment uses.
	h := handler(t, httpserver.Config{AuthToken: goodToken, AllowedHosts: []string{"Sync-MCP.example.com"}})
	for host, want := range map[string]int{
		"sync-mcp.example.com":      200,
		"SYNC-MCP.EXAMPLE.COM:443":  200,
		"sync-mcp.example.com.":     200,
		"evil.example.com":          403,
		"sync-mcp.example.com.evil": 403,
		"127.0.0.1:8081":            403,
		"":                          403,
	} {
		hdr := mcpHeaders(auth)
		hdr["Host"] = host
		if got := do(h, "POST", "/mcp", hdr, initBody).Code; got != want {
			t.Errorf("Host %q: status %d, want %d", host, got, want)
		}
	}
}

func TestOriginValidation(t *testing.T) {
	auth := map[string]string{"Authorization": "Bearer " + goodToken}
	h := handler(t, httpserver.Config{AuthToken: goodToken, AllowedOrigins: []string{"https://app.example.com"}})
	for origin, want := range map[string]int{
		"":                         200, // non-browser clients send none
		"https://app.example.com":  200,
		"https://APP.example.com/": 200,
		"https://evil.example.com": 403,
		"http://app.example.com":   403,
		"null":                     403,
	} {
		hdr := mcpHeaders(auth)
		if origin != "" {
			hdr["Origin"] = origin
		}
		if got := do(h, "POST", "/mcp", hdr, initBody).Code; got != want {
			t.Errorf("Origin %q: status %d, want %d", origin, got, want)
		}
	}

	// With no allowlist configured, every browser origin is refused, even
	// with a valid token.
	h = handler(t, httpserver.Config{AuthToken: goodToken})
	hdr := mcpHeaders(auth)
	hdr["Origin"] = "https://anything.example.com"
	if got := do(h, "POST", "/mcp", hdr, initBody).Code; got != 403 {
		t.Errorf("unlisted Origin: status %d, want 403", got)
	}

	// Host/Origin are checked before the token, so a rebinding page gets 403, not a token oracle.
	hdr = mcpHeaders(nil)
	hdr["Origin"] = "https://anything.example.com"
	if got := do(h, "POST", "/mcp", hdr, initBody).Code; got != 403 {
		t.Errorf("bad Origin without token: status %d, want 403", got)
	}
}

func TestBodyLimit(t *testing.T) {
	h := handler(t, httpserver.Config{AuthToken: goodToken})
	hdr := mcpHeaders(map[string]string{"Authorization": "Bearer " + goodToken})
	big := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo","arguments":{"text":"` + strings.Repeat("a", httpserver.MaxBodyBytes+10) + `"}}}`
	rec := do(h, "POST", "/mcp", hdr, big)
	if rec.Code < 400 {
		t.Fatalf("oversized body accepted: %d", rec.Code)
	}
}

func TestServerTimeoutsAndLimits(t *testing.T) {
	srv, err := httpserver.New(newMCP(), httpserver.Config{Addr: "127.0.0.1:0", AuthToken: goodToken, UpstreamTimeout: 20 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if srv.ReadHeaderTimeout <= 0 || srv.ReadTimeout <= 0 || srv.IdleTimeout <= 0 {
		t.Errorf("timeouts not set: %v %v %v", srv.ReadHeaderTimeout, srv.ReadTimeout, srv.IdleTimeout)
	}
	if srv.WriteTimeout < 2*20*time.Second {
		t.Errorf("WriteTimeout %v is shorter than two upstream calls", srv.WriteTimeout)
	}
	if srv.MaxHeaderBytes <= 0 || srv.MaxHeaderBytes > 64<<10 {
		t.Errorf("MaxHeaderBytes = %d", srv.MaxHeaderBytes)
	}
}

func TestAuthFailureLogNeverContainsToken(t *testing.T) {
	var buf bytes.Buffer
	h := handler(t, httpserver.Config{AuthToken: goodToken, Logger: log.New(&buf, "", 0)})
	hdr := mcpHeaders(map[string]string{"Authorization": "Bearer attacker-supplied-secret-value"})
	for i := 0; i < 5; i++ {
		do(h, "POST", "/mcp", hdr, initBody)
	}
	do(h, "POST", "/mcp", mcpHeaders(map[string]string{"Authorization": "Bearer " + goodToken}), initBody)
	out := buf.String()
	if strings.Contains(out, goodToken) || strings.Contains(out, "attacker-supplied") {
		t.Errorf("log contains a token: %q", out)
	}
	if n := strings.Count(out, "rejected"); n != 1 {
		t.Errorf("expected exactly one rate-limited failure log line, got %d: %q", n, out)
	}
}

// startServer runs a real listener and returns its base URL.
func startServer(t *testing.T, cfg httpserver.Config) (base string, stop func() error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Addr = ln.Addr().String()
	srv, err := httpserver.New(newMCP(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- httpserver.Serve(ctx, srv, ln) }()
	t.Cleanup(cancel)
	return "http://" + ln.Addr().String(), func() error { cancel(); return <-done }
}

func TestMCPClientOverHTTP(t *testing.T) {
	base, stop := startServer(t, httpserver.Config{AuthToken: goodToken})

	connect := func(token string) (*mcp.ClientSession, error) {
		hc := &http.Client{Transport: roundTripper(func(r *http.Request) (*http.Response, error) {
			if token != "" {
				r.Header.Set("Authorization", "Bearer "+token)
			}
			return http.DefaultTransport.RoundTrip(r)
		})}
		tr := &mcp.StreamableClientTransport{Endpoint: base + "/mcp", HTTPClient: hc, DisableStandaloneSSE: true}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return mcp.NewClient(&mcp.Implementation{Name: "c", Version: "0"}, nil).Connect(ctx, tr, nil)
	}

	if cs, err := connect(""); err == nil {
		_ = cs.Close()
		t.Fatal("client without a token connected")
	}
	if cs, err := connect("wrong-wrong-wrong-wrong"); err == nil {
		_ = cs.Close()
		t.Fatal("client with a wrong token connected")
	}
	cs, err := connect(goodToken)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: "echo", Arguments: map[string]any{"text": "hello"}})
	if err != nil || res.IsError {
		t.Fatalf("call: %v %+v", err, res)
	}
	if m, _ := res.StructuredContent.(map[string]any); m["echo"] != "hello" {
		t.Errorf("structured = %v", res.StructuredContent)
	}

	// Graceful shutdown on context cancellation (what SIGTERM triggers).
	if err := stop(); err != nil {
		t.Errorf("Serve returned %v on shutdown", err)
	}
	if _, err := http.Get(base + "/healthz"); err == nil {
		t.Error("server still accepting connections after shutdown")
	}
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestServeShutdownWaitsForInFlightRequest(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		_, _ = io.WriteString(w, "done")
	})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- httpserver.Serve(ctx, srv, ln) }()

	got := make(chan string, 1)
	go func() {
		resp, err := http.Get("http://" + ln.Addr().String())
		if err != nil {
			got <- "error: " + err.Error()
			return
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		got <- string(b)
	}()
	<-started
	cancel()
	select {
	case <-done:
		t.Fatal("Serve returned before the in-flight request finished")
	case <-time.After(150 * time.Millisecond):
	}
	close(release)
	if body := <-got; body != "done" {
		t.Errorf("in-flight response = %q", body)
	}
	if err := <-done; err != nil {
		t.Errorf("Serve: %v", err)
	}
}

func TestInFlightLimit(t *testing.T) {
	// Fill the semaphore with blocked requests through the real handler chain
	// by using a tool that blocks.
	release := make(chan struct{})
	entered := make(chan struct{}, httpserver.MaxInFlight+1)
	s := mcp.NewServer(&mcp.Implementation{Name: "t", Version: "0"}, nil)
	type in struct{}
	type out struct{}
	mcp.AddTool(s, &mcp.Tool{Name: "block", Description: "block"}, func(ctx context.Context, _ *mcp.CallToolRequest, _ in) (*mcp.CallToolResult, out, error) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, out{}, nil
	})
	srv, err := httpserver.New(s, httpserver.Config{Addr: "127.0.0.1:0", AuthToken: goodToken})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler)
	defer ts.Close()
	defer close(release)

	call := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"block","arguments":{}}}`
	post := func() int {
		req, _ := http.NewRequest("POST", ts.URL+"/mcp", strings.NewReader(call))
		req.Header.Set("Authorization", "Bearer "+goodToken)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return -1
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode
	}
	for i := 0; i < httpserver.MaxInFlight; i++ {
		go post()
	}
	for i := 0; i < httpserver.MaxInFlight; i++ {
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d requests entered the handler", i)
		}
	}
	if got := post(); got != http.StatusServiceUnavailable {
		t.Errorf("request over the limit: status %d, want 503", got)
	}
}
