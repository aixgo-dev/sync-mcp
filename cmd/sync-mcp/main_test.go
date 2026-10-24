package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func envFrom(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestVersionFlag(t *testing.T) {
	var out bytes.Buffer
	if err := run([]string{"--version"}, envFrom(nil), strings.NewReader(""), &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(out.String()) != version {
		t.Errorf("version output %q", out.String())
	}
}

func TestMissingConfig(t *testing.T) {
	err := run(nil, envFrom(map[string]string{"SYNC_BASE_URL": "http://localhost:1"}), strings.NewReader(""), io.Discard, io.Discard)
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"SYNC_API_KEY", "SYNC_ORG", "SYNC_PROJECT"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should mention %s", err, want)
		}
	}
	if strings.Contains(err.Error(), "SYNC_BASE_URL") {
		t.Errorf("error %q should not mention the provided SYNC_BASE_URL", err)
	}
}

func TestInvalidConfig(t *testing.T) {
	env := envFrom(map[string]string{"SYNC_BASE_URL": "http://localhost:1", "SYNC_API_KEY": "k", "SYNC_ORG": "bad/org", "SYNC_PROJECT": "p"})
	if err := run(nil, env, strings.NewReader(""), io.Discard, io.Discard); err == nil {
		t.Fatal("expected invalid org to be rejected")
	}
}

// TestStdioLoop drives the real run loop with newline-delimited JSON-RPC.
func TestStdioLoop(t *testing.T) {
	const secret = "stdio-secret-key"
	sync := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/healthz":
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case r.Header.Get("Authorization") != "Bearer "+secret:
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		case r.URL.Path == "/v1/orgs/org1/projects/proj1/jobs/abc":
			w.Header().Set("ETag", `"e1"`)
			_, _ = w.Write([]byte(`{"id":"abc","kind":"custom","status":"queued","created_at":"c","updated_at":"u"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer sync.Close()

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	var stderr bytes.Buffer
	done := make(chan error, 1)
	go func() {
		env := envFrom(map[string]string{"SYNC_BASE_URL": sync.URL, "SYNC_API_KEY": secret, "SYNC_ORG": "org1", "SYNC_PROJECT": "proj1"})
		done <- run(nil, env, inR, outW, &stderr)
		_ = outW.Close()
	}()

	reader := bufio.NewReader(outR)
	send := func(v any) {
		b, _ := json.Marshal(v)
		if _, err := inW.Write(append(b, '\n')); err != nil {
			t.Fatal(err)
		}
	}
	recv := func() map[string]any {
		type result struct {
			m   map[string]any
			err error
		}
		ch := make(chan result, 1)
		go func() {
			line, err := reader.ReadBytes('\n')
			var m map[string]any
			if err == nil {
				err = json.Unmarshal(line, &m)
			}
			ch <- result{m, err}
		}()
		select {
		case r := <-ch:
			if r.err != nil {
				t.Fatalf("reading response: %v", r.err)
			}
			return r.m
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for response")
			return nil
		}
	}

	send(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": map[string]any{
		"protocolVersion": "2025-06-18", "capabilities": map[string]any{},
		"clientInfo": map[string]any{"name": "test", "version": "0"},
	}})
	init := recv()
	result, _ := init["result"].(map[string]any)
	if result == nil || result["serverInfo"].(map[string]any)["name"] != "sync-mcp" {
		t.Fatalf("bad initialize response: %v", init)
	}
	send(map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"})

	send(map[string]any{"jsonrpc": "2.0", "id": 2, "method": "ping"})
	if r := recv(); r["id"] != float64(2) || r["error"] != nil {
		t.Fatalf("bad ping response: %v", r)
	}

	send(map[string]any{"jsonrpc": "2.0", "id": 3, "method": "tools/list"})
	list := recv()["result"].(map[string]any)["tools"].([]any)
	if len(list) != 8 {
		t.Errorf("tools/list returned %d tools, want 8", len(list))
	}

	send(map[string]any{"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": map[string]any{
		"name": "sync_job_get", "arguments": map[string]any{"job_id": "abc"}}})
	call := recv()["result"].(map[string]any)
	if call["isError"] == true {
		t.Fatalf("unexpected tool error: %v", call)
	}
	sc := call["structuredContent"].(map[string]any)
	if sc["etag"] != "e1" || sc["job"].(map[string]any)["id"] != "abc" {
		t.Errorf("unexpected structured content %v", sc)
	}

	send(map[string]any{"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": map[string]any{
		"name": "sync_job_get", "arguments": map[string]any{"job_id": "nope"}}})
	failed := recv()["result"].(map[string]any)
	if failed["isError"] != true {
		t.Errorf("expected isError for 404, got %v", failed)
	}

	_ = inW.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("run returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("server did not exit after stdin closed")
	}
	if strings.Contains(stderr.String(), secret) {
		t.Errorf("stderr leaks the API key: %s", stderr.String())
	}
}
