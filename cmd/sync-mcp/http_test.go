package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	httpToken = "0123456789abcdef0123456789abcdef"
	syncKey   = "sync-api-key-for-tests"
)

func httpEnv(sync string, extra map[string]string) func(string) string {
	m := map[string]string{
		"SYNC_BASE_URL": sync, "SYNC_API_KEY": syncKey, "SYNC_ORG": "org1", "SYNC_PROJECT": "proj1",
		"SYNC_MCP_TRANSPORT": "http", "SYNC_MCP_LISTEN": "127.0.0.1:0",
	}
	for k, v := range extra {
		m[k] = v
	}
	return envFrom(m)
}

func runErr(args []string, env func(string) string) (error, string) {
	var stderr bytes.Buffer
	err := run(args, env, strings.NewReader(""), io.Discard, &stderr)
	return err, stderr.String()
}

func TestHTTPRefusesToStartWithoutToken(t *testing.T) {
	err, _ := runErr(nil, httpEnv("http://127.0.0.1:1", nil))
	if err == nil || !strings.Contains(err.Error(), "SYNC_MCP_AUTH_TOKEN") {
		t.Fatalf("error = %v, want one naming SYNC_MCP_AUTH_TOKEN", err)
	}
}

func TestHTTPTokenValidation(t *testing.T) {
	cases := []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{"short token", nil, map[string]string{"SYNC_MCP_AUTH_TOKEN": "tooshort"}, "at least 16"},
		{"same as sync key", nil, map[string]string{"SYNC_MCP_AUTH_TOKEN": syncKey}, "must differ"},
		{"insecure on all interfaces", []string{"--insecure-no-auth", "--listen", ":8081"}, nil, "loopback"},
		{"insecure on 0.0.0.0 via env", []string{"--insecure-no-auth"}, map[string]string{"SYNC_MCP_LISTEN": "0.0.0.0:8081"}, "loopback"},
		{"insecure on private ip", []string{"--insecure-no-auth", "--listen", "10.0.0.5:8081"}, nil, "loopback"},
		{"insecure plus token", []string{"--insecure-no-auth"}, map[string]string{"SYNC_MCP_AUTH_TOKEN": httpToken}, "cannot be combined"},
		{"bad transport", []string{"--transport", "grpc"}, nil, "stdio or http"},
		{"bad actor id", nil, map[string]string{"SYNC_MCP_AUTH_TOKEN": httpToken, "SYNC_ACTOR_ID": "a b"}, "SYNC_ACTOR_ID"},
		{"bad allowed host", []string{"--allowed-host", "https://x.example.com"}, map[string]string{"SYNC_MCP_AUTH_TOKEN": httpToken}, "allowed host"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err, _ := runErr(tc.args, httpEnv("http://127.0.0.1:1", tc.env))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want one containing %q", err, tc.want)
			}
			for _, secret := range []string{httpToken, syncKey} {
				if strings.Contains(err.Error(), secret) {
					t.Errorf("error leaks a secret: %v", err)
				}
			}
		})
	}
}

func TestStdioIgnoresHTTPSettings(t *testing.T) {
	// The default transport is stdio and does not need an auth token.
	var out bytes.Buffer
	err := run([]string{"--version"}, envFrom(nil), strings.NewReader(""), &out, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
}

// fakeSyncRecorder is a Sync stand-in that records what it was sent.
type fakeSyncRecorder struct {
	mu     sync.Mutex
	auths  []string
	bodies []string
}

func (f *fakeSyncRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.auths = append(f.auths, r.Header.Get("Authorization"))
	body, _ := io.ReadAll(r.Body)
	f.bodies = append(f.bodies, string(body))
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.URL.Path == "/healthz":
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/messages"):
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"msg_1","thread_id":"t1","role":"agent","content":"x","created_at":"2026-01-01T00:00:00Z"}`))
	default:
		http.NotFound(w, r)
	}
}

// TestHTTPTransportEndToEnd starts the real server through run's code path and
// talks to it with an MCP client over Streamable HTTP.
func TestHTTPTransportEndToEnd(t *testing.T) {
	fake := &fakeSyncRecorder{}
	syncSrv := httptest.NewServer(fake)
	defer syncSrv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	done := make(chan error, 1)
	var stderr bytes.Buffer
	env := httpEnv(syncSrv.URL, map[string]string{"SYNC_MCP_AUTH_TOKEN": httpToken, "SYNC_ACTOR_ID": "act_ci_agent"})
	go func() {
		done <- runContext(ctx, nil, env, strings.NewReader(""), io.Discard, &stderr, ready)
	}()
	var addr string
	select {
	case addr = <-ready:
	case err := <-done:
		t.Fatalf("server exited early: %v\n%s", err, stderr.String())
	case <-time.After(10 * time.Second):
		t.Fatal("server did not start")
	}
	base := "http://" + addr

	// Unauthenticated health probe works and says nothing.
	resp, err := http.Get(base + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	hb, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 || strings.TrimSpace(string(hb)) != "ok" {
		t.Errorf("healthz = %d %q", resp.StatusCode, hb)
	}

	// The MCP endpoint rejects a missing token.
	resp, err = http.Post(base+"/mcp", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 401 || !strings.HasPrefix(resp.Header.Get("WWW-Authenticate"), "Bearer") {
		t.Errorf("no token: %d %q", resp.StatusCode, resp.Header.Get("WWW-Authenticate"))
	}

	hc := &http.Client{Transport: headerTransport{"Authorization": "Bearer " + httpToken}}
	tr := &mcp.StreamableClientTransport{Endpoint: base + "/mcp", HTTPClient: hc, DisableStandaloneSSE: true}
	cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer ccancel()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "c", Version: "0"}, nil).Connect(cctx, tr, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()

	tl, err := cs.ListTools(cctx, nil)
	if err != nil || len(tl.Tools) != 8 {
		t.Fatalf("tools = %d, err %v", len(tl.Tools), err)
	}

	res, err := cs.CallTool(cctx, &mcp.CallToolParams{Name: "sync_message_append", Arguments: map[string]any{
		"thread_id": "t1", "role": "agent", "content": "hello", "sender_actor_id": "act_spoofed",
	}})
	if err != nil || res.IsError {
		t.Fatalf("append: %v %+v", err, res)
	}
	res, err = cs.CallTool(cctx, &mcp.CallToolParams{Name: "sync_health"})
	if err != nil || res.IsError {
		t.Fatalf("health: %v %+v", err, res)
	}

	fake.mu.Lock()
	var sawAppend bool
	for i, b := range fake.bodies {
		if strings.Contains(b, `"hello"`) {
			sawAppend = true
			var m map[string]any
			_ = json.Unmarshal([]byte(b), &m)
			if m["sender_actor_id"] != "act_ci_agent" {
				t.Errorf("Sync received sender_actor_id %v, want the pinned act_ci_agent", m["sender_actor_id"])
			}
			if fake.auths[i] != "Bearer "+syncKey {
				t.Errorf("Sync received Authorization %q, want the Sync API key", fake.auths[i])
			}
		}
		if strings.Contains(fake.auths[i], httpToken) {
			t.Error("the client's MCP token was forwarded to Sync")
		}
	}
	fake.mu.Unlock()
	if !sawAppend {
		t.Error("Sync never received the append")
	}

	// Cancelling the context (SIGTERM in production) shuts down cleanly.
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("shutdown error: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("server did not shut down")
	}
	if strings.Contains(stderr.String(), httpToken) || strings.Contains(stderr.String(), syncKey) {
		t.Errorf("logs contain a secret:\n%s", stderr.String())
	}
	if !strings.Contains(stderr.String(), "serving") {
		t.Errorf("expected a startup log line, got:\n%s", stderr.String())
	}
}

type headerTransport map[string]string

func (h headerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	for k, v := range h {
		r.Header.Set(k, v)
	}
	return http.DefaultTransport.RoundTrip(r)
}

func TestInsecureNoAuthEndToEnd(t *testing.T) {
	syncSrv := httptest.NewServer(&fakeSyncRecorder{})
	defer syncSrv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	done := make(chan error, 1)
	var stderr bytes.Buffer
	go func() {
		done <- runContext(ctx, []string{"--insecure-no-auth"}, httpEnv(syncSrv.URL, nil), strings.NewReader(""), io.Discard, &stderr, ready)
	}()
	select {
	case addr := <-ready:
		tr := &mcp.StreamableClientTransport{Endpoint: "http://" + addr + "/mcp", DisableStandaloneSSE: true}
		cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ccancel()
		cs, err := mcp.NewClient(&mcp.Implementation{Name: "c", Version: "0"}, nil).Connect(cctx, tr, nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = cs.Close()
	case err := <-done:
		t.Fatalf("exited early: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("did not start")
	}
	if !strings.Contains(stderr.String(), "authentication is disabled") {
		t.Errorf("expected a warning about disabled auth, got:\n%s", stderr.String())
	}
	cancel()
	<-done
}

// TestHTTPTransportAgainstRealSync runs the HTTP transport in front of a real
// Sync binary (in-memory store). It is skipped unless SYNC_E2E_BINARY points at
// a built aixgo-sync binary:
//
//	SYNC_E2E_BINARY=/path/to/aixgo-sync go test ./cmd/sync-mcp -run RealSync -v
func TestHTTPTransportAgainstRealSync(t *testing.T) {
	bin := os.Getenv("SYNC_E2E_BINARY")
	if bin == "" {
		t.Skip("SYNC_E2E_BINARY not set")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	var logs bytes.Buffer
	cmd := exec.Command(bin, "serve")
	cmd.Env = append(os.Environ(), fmt.Sprintf("SYNC_PORT=%d", port), "SYNC_API_KEYS=org1:proj1:"+syncKey, "SYNC_S3_BUCKET=")
	cmd.Stdout, cmd.Stderr = &logs, &logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	syncURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		if resp, err := http.Get(syncURL + "/healthz"); err == nil {
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("sync did not start:\n%s", logs.String())
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	done := make(chan error, 1)
	env := httpEnv(syncURL, map[string]string{"SYNC_MCP_AUTH_TOKEN": httpToken, "SYNC_ACTOR_ID": "act_real_e2e"})
	go func() { done <- runContext(ctx, nil, env, strings.NewReader(""), io.Discard, io.Discard, ready) }()
	var addr string
	select {
	case addr = <-ready:
	case err := <-done:
		t.Fatal(err)
	case <-time.After(10 * time.Second):
		t.Fatal("mcp server did not start")
	}

	hc := &http.Client{Transport: headerTransport{"Authorization": "Bearer " + httpToken}}
	tr := &mcp.StreamableClientTransport{Endpoint: "http://" + addr + "/mcp", HTTPClient: hc, DisableStandaloneSSE: true}
	cctx, ccancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer ccancel()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "c", Version: "0"}, nil).Connect(cctx, tr, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	call := func(name string, args map[string]any) map[string]any {
		t.Helper()
		res, err := cs.CallTool(cctx, &mcp.CallToolParams{Name: name, Arguments: args})
		if err != nil || res.IsError {
			t.Fatalf("%s: %v %+v", name, err, res)
		}
		m, _ := res.StructuredContent.(map[string]any)
		return m
	}
	if call("sync_health", nil)["status"] != "ok" {
		t.Error("health not ok")
	}
	call("sync_session_write", map[string]any{"session_id": "s1", "thread_id": "t1"})
	call("sync_message_append", map[string]any{"thread_id": "t1", "role": "agent", "content": "<hi> & bye", "sender_actor_id": "act_spoofed"})
	list := call("sync_message_list", map[string]any{"thread_id": "t1"})
	msgs, _ := list["messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("messages = %v", list)
	}
	m := msgs[0].(map[string]any)
	if m["sender_actor_id"] != "act_real_e2e" || m["content"] != "<hi> & bye" {
		t.Errorf("stored message = %v", m)
	}
	job := call("sync_job_create", map[string]any{"kind": "custom", "payload": map[string]any{"a": 1}})
	id := job["job"].(map[string]any)["id"].(string)
	if call("sync_job_claim", map[string]any{"job_id": id, "worker_id": "w1"})["job"].(map[string]any)["status"] != "claimed" {
		t.Error("job not claimed")
	}
}
