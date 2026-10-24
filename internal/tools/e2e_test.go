package tools_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/aixgo-dev/sync-mcp/internal/syncclient"
	"github.com/aixgo-dev/sync-mcp/internal/tools"
)

// TestEndToEndAgainstRealSync runs every tool against a real Sync server
// binary with an in-memory store. It is skipped unless SYNC_E2E_BINARY points
// at a built aixgo-sync binary:
//
//	SYNC_E2E_BINARY=/path/to/aixgo-sync go test ./internal/tools -run EndToEnd -v
func TestEndToEndAgainstRealSync(t *testing.T) {
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
	cmd.Env = append(os.Environ(), fmt.Sprintf("SYNC_PORT=%d", port), "SYNC_API_KEYS=org1:proj1:e2e-token", "SYNC_S3_BUCKET=")
	cmd.Stdout, cmd.Stderr = &logs, &logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })

	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := http.Get(base + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("sync did not start: %v\n%s", err, logs.String())
		}
		time.Sleep(100 * time.Millisecond)
	}

	connect := func(org, project, token string) *mcp.ClientSession {
		client, err := syncclient.New(syncclient.Config{BaseURL: base, APIKey: token, Org: org, Project: project})
		if err != nil {
			t.Fatal(err)
		}
		server := mcp.NewServer(&mcp.Implementation{Name: "sync-mcp", Version: "e2e"}, nil)
		tools.Register(server, client)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		st, ct := mcp.NewInMemoryTransports()
		if _, err := server.Connect(ctx, st, nil); err != nil {
			t.Fatal(err)
		}
		cs, err := mcp.NewClient(&mcp.Implementation{Name: "c", Version: "e2e"}, nil).Connect(ctx, ct, nil)
		if err != nil {
			t.Fatal(err)
		}
		return cs
	}

	h := &harness{session: connect("org1", "proj1", "e2e-token")}

	if h.ok(t, tools.ToolHealth, nil)["status"] != "ok" {
		t.Error("health not ok")
	}

	// Sessions: create, read, CAS update, conflict, stale etag.
	created := h.ok(t, tools.ToolSessionWrite, map[string]any{
		"session_id": "s1", "thread_id": "t1", "labels": map[string]any{"k": "v"},
	})
	etag1 := created["etag"].(string)
	read := h.ok(t, tools.ToolSessionRead, map[string]any{"session_id": "s1"})
	if read["etag"] != etag1 {
		t.Errorf("etag mismatch: %v vs %v", read["etag"], etag1)
	}
	sess := sub(t, read, "session")
	updated := h.ok(t, tools.ToolSessionWrite, map[string]any{
		"session_id": "s1", "thread_id": "t1", "status": "closed", "worker_id": "w1",
		"created_at": sess["created_at"], "if_match": etag1,
	})
	if updated["etag"] == etag1 {
		t.Error("etag unchanged after update")
	}
	h.fail(t, tools.ToolSessionWrite, map[string]any{"session_id": "s1", "thread_id": "t1"}, "409")
	h.fail(t, tools.ToolSessionWrite, map[string]any{"session_id": "s1", "thread_id": "t1", "if_match": etag1}, "412")
	h.fail(t, tools.ToolSessionRead, map[string]any{"session_id": "missing"}, "404")

	// Messages.
	var first string
	for i := 0; i < 3; i++ {
		out := h.ok(t, tools.ToolMessageAppend, map[string]any{
			"thread_id": "t1", "role": "agent", "content": fmt.Sprintf("m%d", i), "sender_actor_id": "a1",
		})
		if first == "" {
			first = sub(t, out, "message")["id"].(string)
		}
	}
	list := h.ok(t, tools.ToolMessageList, map[string]any{"thread_id": "t1"})
	if list["count"].(float64) != 3 {
		t.Errorf("expected 3 messages, got %v", list)
	}
	page := h.ok(t, tools.ToolMessageList, map[string]any{"thread_id": "t1", "after_id": first, "limit": 1})
	if page["count"].(float64) != 1 {
		t.Errorf("expected 1 message in page, got %v", page)
	}

	// Jobs.
	job := sub(t, h.ok(t, tools.ToolJobCreate, map[string]any{"kind": "command", "payload": map[string]any{"cmd": "true"}}), "job")
	id := job["id"].(string)
	if got := sub(t, h.ok(t, tools.ToolJobGet, map[string]any{"job_id": id}), "job"); got["status"] != "queued" {
		t.Errorf("unexpected job %v", got)
	}
	claimed := sub(t, h.ok(t, tools.ToolJobClaim, map[string]any{"job_id": id, "worker_id": "w1"}), "job")
	if claimed["status"] != "claimed" || claimed["assignee_worker"] != "w1" {
		t.Errorf("unexpected claim %v", claimed)
	}
	h.fail(t, tools.ToolJobClaim, map[string]any{"job_id": id, "worker_id": "w2"}, "409")
	h.fail(t, tools.ToolJobGet, map[string]any{"job_id": "deadbeef"}, "404")

	// Auth failures against the real server: wrong key -> 401; right key but
	// a different tenant than the key's scope -> 403.
	bad := &harness{session: connect("org1", "proj1", "not-the-token")}
	bad.fail(t, tools.ToolJobGet, map[string]any{"job_id": id}, "401")
	other := &harness{session: connect("org2", "proj2", "e2e-token")}
	other.fail(t, tools.ToolJobGet, map[string]any{"job_id": id}, "403")
	// /healthz stays open even for a bad key.
	if bad.ok(t, tools.ToolHealth, nil)["status"] != "ok" {
		t.Error("health should not need a key")
	}
}
