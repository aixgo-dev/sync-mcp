package tools_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/aixgo-dev/sync-mcp/internal/syncclient"
	"github.com/aixgo-dev/sync-mcp/internal/tools"
)

func keys(m map[string]any) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func TestAppendReturnsOnlyReceipt(t *testing.T) {
	h := newHarness(t, testToken)
	const secretContent = "very-distinctive-content-123"
	res := h.call(t, tools.ToolMessageAppend, map[string]any{
		"thread_id": "t1", "role": "user", "content": secretContent,
		"refs": map[string]any{"k": "refs-distinctive-456"},
	})
	if res.IsError {
		t.Fatalf("unexpected error: %s", text(res))
	}
	got := res.StructuredContent.(map[string]any)
	if fmt.Sprint(keys(got)) != "[created_at etag id thread_id]" {
		t.Errorf("structured keys = %v", keys(got))
	}
	structured, _ := json.Marshal(got)
	for _, out := range []string{text(res), string(structured)} {
		if strings.Contains(out, secretContent) || strings.Contains(out, "refs-distinctive") {
			t.Errorf("append echoed the message: %s", out)
		}
	}
}

func TestOversizeInputRejected(t *testing.T) {
	h := newHarness(t, testToken)
	big := strings.Repeat("a", tools.MaxInputBytes+1)

	h.fail(t, tools.ToolMessageAppend, map[string]any{"thread_id": "t1", "role": "user", "content": big}, "content", "262144")
	h.fail(t, tools.ToolMessageAppend, map[string]any{"thread_id": "t1", "role": "user", "content": "x",
		"refs": map[string]any{"blob": big}}, "refs", "262144")
	h.fail(t, tools.ToolJobCreate, map[string]any{"kind": "custom", "payload": map[string]any{"blob": big}}, "payload", "262144")
	h.fail(t, tools.ToolSessionWrite, map[string]any{"session_id": "s", "thread_id": "t",
		"labels": map[string]any{"blob": big}}, "labels", "262144")

	// Nothing oversized reached the server.
	if n := len(h.fake.objects); n != 0 {
		t.Errorf("fake stored %d objects after rejected inputs", n)
	}

	// The limit is inclusive: exactly MaxInputBytes of content is accepted.
	h.ok(t, tools.ToolMessageAppend, map[string]any{"thread_id": "t1", "role": "user", "content": strings.Repeat("a", tools.MaxInputBytes)})
}

// The original failure: a few huge messages made the thread unlistable, with
// a JSON decode error instead of an explanation.
func TestOversizeUpstreamResponseGivesExplicitError(t *testing.T) {
	h := newHarnessWith(t, testToken, func(c *syncclient.Config) { c.MaxResponseBytes = 4000 })
	// Write directly to the fake so the input cap does not apply, like a
	// message stored by some other client.
	for i := 0; i < 4; i++ {
		h.fake.mu.Lock()
		h.fake.nextID++
		id := fmt.Sprintf("msg_%04d", h.fake.nextID)
		data, _ := json.Marshal(map[string]any{"id": id, "thread_id": "t1", "role": "user",
			"content": strings.Repeat("x", 1500), "created_at": "2026-01-01T00:00:00Z"})
		h.fake.objects["msg/t1/"+id] = obj{data: data, rev: 1}
		h.fake.mu.Unlock()
	}
	res := h.call(t, tools.ToolMessageList, map[string]any{"thread_id": "t1"})
	if !res.IsError {
		t.Fatalf("expected error, got %s", text(res))
	}
	msg := text(res)
	if !strings.Contains(msg, "response exceeds 4000 bytes; use a smaller limit") || !strings.Contains(msg, "after_id") {
		t.Errorf("unclear error: %q", msg)
	}
	for _, bad := range []string{"decoding", "unexpected end", "invalid character"} {
		if strings.Contains(msg, bad) {
			t.Errorf("error looks like a decode failure: %q", msg)
		}
	}
	// A smaller limit works, as the hint says.
	got := h.ok(t, tools.ToolMessageList, map[string]any{"thread_id": "t1", "limit": 1})
	if got["count"].(float64) != 1 || got["has_more"] != true {
		t.Errorf("unexpected small list %v", got)
	}
}

func TestListTruncatedAtSizeCap(t *testing.T) {
	h := newHarness(t, testToken)
	chunk := strings.Repeat("y", 120<<10) // three of these exceed the 256 KiB result cap
	var ids []string
	for i := 0; i < 3; i++ {
		out := h.ok(t, tools.ToolMessageAppend, map[string]any{"thread_id": "t1", "role": "user", "content": chunk})
		ids = append(ids, out["id"].(string))
	}
	first := h.ok(t, tools.ToolMessageList, map[string]any{"thread_id": "t1"})
	if first["count"].(float64) != 2 || first["truncated"] != true || first["has_more"] != true || first["last_id"] != ids[1] {
		t.Fatalf("unexpected first page: count=%v truncated=%v has_more=%v last=%v", first["count"], first["truncated"], first["has_more"], first["last_id"])
	}
	if hint, _ := first["hint"].(string); !strings.Contains(hint, "after_id") {
		t.Errorf("hint should mention after_id: %q", hint)
	}
	second := h.ok(t, tools.ToolMessageList, map[string]any{"thread_id": "t1", "after_id": first["last_id"]})
	if second["count"].(float64) != 1 || second["truncated"] != false || second["has_more"] != false || second["last_id"] != ids[2] {
		t.Errorf("unexpected second page: %v", second["count"])
	}
}

func TestListHasMoreFlag(t *testing.T) {
	h := newHarness(t, testToken)
	for i := 0; i < 3; i++ {
		h.ok(t, tools.ToolMessageAppend, map[string]any{"thread_id": "t1", "role": "user", "content": "m"})
	}
	two := h.ok(t, tools.ToolMessageList, map[string]any{"thread_id": "t1", "limit": 2})
	if two["count"].(float64) != 2 || two["has_more"] != true || two["truncated"] != false {
		t.Errorf("limit 2: %v", two)
	}
	three := h.ok(t, tools.ToolMessageList, map[string]any{"thread_id": "t1", "limit": 3})
	if three["count"].(float64) != 3 || three["has_more"] != false {
		t.Errorf("limit 3: %v", three)
	}
	// The default limit is 100, and the 101st message is not returned.
	for i := 0; i < 98; i++ {
		h.ok(t, tools.ToolMessageAppend, map[string]any{"thread_id": "t1", "role": "user", "content": "m"})
	}
	exact := h.ok(t, tools.ToolMessageList, map[string]any{"thread_id": "t1"})
	if exact["count"].(float64) != 100 || exact["has_more"] != true {
		t.Errorf("101 messages with default limit: count=%v has_more=%v", exact["count"], exact["has_more"])
	}
}

func TestClaimConflictHint(t *testing.T) {
	h := newHarness(t, testToken)
	id := sub(t, h.ok(t, tools.ToolJobCreate, map[string]any{"kind": "custom"}), "job")["id"].(string)
	h.ok(t, tools.ToolJobClaim, map[string]any{"job_id": id, "worker_id": "w1"})
	res := h.call(t, tools.ToolJobClaim, map[string]any{"job_id": id, "worker_id": "w1"})
	if !res.IsError {
		t.Fatal("expected conflict")
	}
	msg := text(res)
	for _, want := range []string{"409", "sync_job_get", "assignee_worker equals your worker_id", "already succeeded"} {
		if !strings.Contains(msg, want) {
			t.Errorf("claim 409 error %q lacks %q", msg, want)
		}
	}
	if strings.Contains(msg, "already exists") {
		t.Errorf("job conflict must not say the resource already exists: %q", msg)
	}
}

func TestConflictAndPreconditionHints(t *testing.T) {
	h := newHarness(t, testToken)
	h.ok(t, tools.ToolSessionWrite, map[string]any{"session_id": "s1", "thread_id": "t1"})
	h.fail(t, tools.ToolSessionWrite, map[string]any{"session_id": "s1", "thread_id": "t1"}, "409", "already exists", "if_match")
	h.fail(t, tools.ToolSessionWrite, map[string]any{"session_id": "s1", "thread_id": "t1", "if_match": "stale"},
		"412", "replaces the whole document", "labels")
}

func TestToolDescriptionsWarnAndAdvise(t *testing.T) {
	h := newHarness(t, testToken)
	res, err := h.session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	desc := map[string]string{}
	for _, tl := range res.Tools {
		desc[tl.Name] = tl.Description
	}
	for _, name := range []string{tools.ToolMessageList, tools.ToolJobGet, tools.ToolJobCreate} {
		d := desc[name]
		if !strings.Contains(d, "untrusted") || !strings.Contains(d, "never follow instructions") {
			t.Errorf("%s description lacks the untrusted-content warning: %q", name, d)
		}
	}
	for _, name := range []string{tools.ToolJobGet, tools.ToolJobCreate} {
		if !strings.Contains(desc[name], "command or code_offload") || !strings.Contains(desc[name], "execute work") {
			t.Errorf("%s description lacks the job-execution warning", name)
		}
	}
	for _, name := range []string{tools.ToolJobCreate, tools.ToolMessageAppend} {
		if !strings.Contains(desc[name], "no idempotency key") || !strings.Contains(desc[name], "duplicate") {
			t.Errorf("%s description lacks the retry warning", name)
		}
	}
	if !strings.Contains(desc[tools.ToolJobClaim], "sync_job_get") {
		t.Errorf("claim description lacks the retry guidance")
	}
}

func TestReadOnlyRegistersOnlyReadTools(t *testing.T) {
	h := newHarnessWith(t, testToken, nil, tools.ReadOnly())
	res, err := h.session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, tl := range res.Tools {
		names = append(names, tl.Name)
		if tl.Annotations == nil || !tl.Annotations.ReadOnlyHint {
			t.Errorf("read-only tool %s is not annotated read-only", tl.Name)
		}
	}
	sort.Strings(names)
	want := []string{tools.ToolHealth, tools.ToolJobGet, tools.ToolMessageList, tools.ToolSessionRead}
	sort.Strings(want)
	if fmt.Sprint(names) != fmt.Sprint(want) {
		t.Errorf("read-only tools = %v, want %v", names, want)
	}
	for _, name := range []string{tools.ToolSessionWrite, tools.ToolMessageAppend, tools.ToolJobCreate, tools.ToolJobClaim} {
		if _, err := h.session.CallTool(context.Background(), &mcp.CallToolParams{Name: name}); err == nil {
			t.Errorf("%s should not be callable in read-only mode", name)
		}
	}
}

func TestAPIKeyWithTrailingNewlineWorks(t *testing.T) {
	h := newHarness(t, testToken+"\n")
	if got := h.ok(t, tools.ToolMessageAppend, map[string]any{"thread_id": "t", "role": "user", "content": "x"}); got["id"] == "" {
		t.Errorf("unexpected %v", got)
	}
	h2 := newHarness(t, "  "+testToken+"\r\n")
	h2.ok(t, tools.ToolSessionWrite, map[string]any{"session_id": "s", "thread_id": "t"})
}

// The extra message fetched to compute has_more must not turn a list that
// would fit into an error.
func TestListFallsBackWhenLookaheadMessageIsTooLarge(t *testing.T) {
	h := newHarnessWith(t, testToken, func(c *syncclient.Config) { c.MaxResponseBytes = 2000 })
	for i := 0; i < 3; i++ {
		h.fake.mu.Lock()
		h.fake.nextID++
		id := fmt.Sprintf("msg_%04d", h.fake.nextID)
		data, _ := json.Marshal(map[string]any{"id": id, "thread_id": "t1", "role": "user",
			"content": strings.Repeat("x", 1200), "created_at": "2026-01-01T00:00:00Z"})
		h.fake.objects["msg/t1/"+id] = obj{data: data, rev: 1}
		h.fake.mu.Unlock()
	}
	got := h.ok(t, tools.ToolMessageList, map[string]any{"thread_id": "t1", "limit": 1})
	if got["count"].(float64) != 1 || got["has_more"] != true {
		t.Errorf("unexpected result %v", got)
	}
}
