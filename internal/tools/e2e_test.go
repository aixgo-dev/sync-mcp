package tools_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
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

	connectWith := func(org, project, token string, maxResp int64, opts ...tools.Option) *mcp.ClientSession {
		client, err := syncclient.New(syncclient.Config{BaseURL: base, APIKey: token, Org: org, Project: project, MaxResponseBytes: maxResp})
		if err != nil {
			t.Fatal(err)
		}
		server := mcp.NewServer(&mcp.Implementation{Name: "sync-mcp", Version: "e2e"}, nil)
		tools.Register(server, client, opts...)
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

	connect := func(org, project, token string) *mcp.ClientSession {
		return connectWith(org, project, token, 0)
	}

	// A trailing newline on the key (as read from a file) still authenticates.
	h := &harness{session: connect("org1", "proj1", "e2e-token\n")}

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
			first = out["id"].(string)
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

	// Regression for the original failure: huge messages must not be able to
	// make a thread unlistable.
	huge := strings.Repeat("m", 3<<20)
	for i := 0; i < 4; i++ {
		h.fail(t, tools.ToolMessageAppend, map[string]any{"thread_id": "huge", "role": "user", "content": huge}, "262144")
	}
	if got := h.ok(t, tools.ToolMessageList, map[string]any{"thread_id": "huge"}); got["count"].(float64) != 0 {
		t.Errorf("rejected messages were stored: %v", got)
	}

	// Messages that fit the input cap but add up: the list result is capped
	// and flagged, and paging with after_id reaches every message.
	chunk := strings.Repeat("c", 200<<10)
	var big []string
	for i := 0; i < 3; i++ {
		out := h.ok(t, tools.ToolMessageAppend, map[string]any{"thread_id": "big", "role": "user", "content": chunk})
		big = append(big, out["id"].(string))
	}
	var seen []string
	after := ""
	for page := 0; page < 5; page++ {
		args := map[string]any{"thread_id": "big"}
		if after != "" {
			args["after_id"] = after
		}
		got := h.ok(t, tools.ToolMessageList, args)
		if page == 0 && (got["truncated"] != true || got["count"].(float64) != 1) {
			t.Errorf("first page should be truncated to 1 message: count=%v truncated=%v", got["count"], got["truncated"])
		}
		for _, m := range got["messages"].([]any) {
			seen = append(seen, m.(map[string]any)["id"].(string))
		}
		if got["has_more"] != true {
			break
		}
		after = got["last_id"].(string)
	}
	if strings.Join(seen, ",") != strings.Join(big, ",") {
		t.Errorf("paging saw %v, want %v", seen, big)
	}

	// An upstream response over the client's size limit is an explicit error,
	// not a JSON decode failure, and a smaller limit works.
	small := &harness{session: connectWith("org1", "proj1", "e2e-token", 300000)}
	res := small.call(t, tools.ToolMessageList, map[string]any{"thread_id": "big"})
	if !res.IsError || !strings.Contains(text(res), "response exceeds 300000 bytes; use a smaller limit") || strings.Contains(text(res), "decoding") {
		t.Errorf("expected explicit size error, got isError=%v %q", res.IsError, text(res))
	}
	if got := small.ok(t, tools.ToolMessageList, map[string]any{"thread_id": "big", "limit": 1}); got["count"].(float64) != 1 || got["has_more"] != true {
		t.Errorf("limit 1 should work: %v", got)
	}

	// Claim retry guidance against the real server.
	retryJob := sub(t, h.ok(t, tools.ToolJobCreate, map[string]any{"kind": "custom"}), "job")["id"].(string)
	h.ok(t, tools.ToolJobClaim, map[string]any{"job_id": retryJob, "worker_id": "w9"})
	h.fail(t, tools.ToolJobClaim, map[string]any{"job_id": retryJob, "worker_id": "w9"}, "409", "sync_job_get", "assignee_worker")
	if sub(t, h.ok(t, tools.ToolJobGet, map[string]any{"job_id": retryJob}), "job")["assignee_worker"] != "w9" {
		t.Error("assignee_worker should be w9")
	}

	// Read-only mode only exposes read tools, and they work.
	ro := &harness{session: connectWith("org1", "proj1", "e2e-token", 0, tools.ReadOnly())}
	listed, err := ro.session.ListTools(context.Background(), nil)
	if err != nil || len(listed.Tools) != 4 {
		t.Errorf("read-only tools: %v err=%v", listed, err)
	}
	ro.ok(t, tools.ToolJobGet, map[string]any{"job_id": retryJob})
}
