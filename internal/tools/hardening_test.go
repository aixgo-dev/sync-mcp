package tools_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/aixgo-dev/sync-mcp/internal/tools"
)

// toolSchema returns the published input schema of one tool as generic JSON.
func toolSchema(t *testing.T, h *harness, name string) map[string]any {
	t.Helper()
	res, err := h.session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tl := range res.Tools {
		if tl.Name == name {
			b, _ := json.Marshal(tl.InputSchema)
			var m map[string]any
			if err := json.Unmarshal(b, &m); err != nil {
				t.Fatal(err)
			}
			return m
		}
	}
	t.Fatalf("tool %q not found", name)
	return nil
}

func prop(t *testing.T, schema map[string]any, name string) map[string]any {
	t.Helper()
	p, ok := schema["properties"].(map[string]any)[name].(map[string]any)
	if !ok {
		t.Fatalf("no property %q in %v", name, schema)
	}
	return p
}

func enumStrings(t *testing.T, p map[string]any) []string {
	t.Helper()
	raw, ok := p["enum"].([]any)
	if !ok {
		t.Fatalf("no enum in %v", p)
	}
	var out []string
	for _, v := range raw {
		out = append(out, v.(string))
	}
	return out
}

func TestSchemasPublishEnums(t *testing.T) {
	h := newHarness(t, testToken)

	got := strings.Join(enumStrings(t, prop(t, toolSchema(t, h, tools.ToolMessageAppend), "role")), ",")
	if want := "user,assistant,system,tool,agent"; got != want {
		t.Errorf("role enum = %s, want %s", got, want)
	}
	got = strings.Join(enumStrings(t, prop(t, toolSchema(t, h, tools.ToolSessionWrite), "status")), ",")
	if want := "open,closed"; got != want {
		t.Errorf("status enum = %s, want %s", got, want)
	}
	got = strings.Join(enumStrings(t, prop(t, toolSchema(t, h, tools.ToolJobCreate), "kind")), ",")
	if want := "code_offload,command,schedule,custom"; got != want {
		t.Errorf("kind enum = %s, want %s", got, want)
	}
	limit := prop(t, toolSchema(t, h, tools.ToolMessageList), "limit")
	if limit["minimum"] != float64(1) || limit["maximum"] != float64(1000) || limit["default"] != float64(20) {
		t.Errorf("limit schema = %v", limit)
	}

	// Values outside the enums are rejected before anything reaches Sync.
	h.fail(t, tools.ToolMessageAppend, map[string]any{"thread_id": "t", "role": "robot", "content": "x"}, "role")
	h.fail(t, tools.ToolSessionWrite, map[string]any{"session_id": "s", "thread_id": "t", "status": "archived"}, "status")
	h.fail(t, tools.ToolJobCreate, map[string]any{"kind": "shell"}, "kind")
	if n := len(h.fake.objects); n != 0 {
		t.Errorf("fake stored %d objects after rejected inputs", n)
	}
}

func storedSender(t *testing.T, h *harness, key string) (string, bool) {
	t.Helper()
	o, ok := h.fake.objects[key]
	if !ok {
		t.Fatalf("no stored object %q", key)
	}
	var m map[string]any
	if err := json.Unmarshal(o.data, &m); err != nil {
		t.Fatal(err)
	}
	s, present := m["sender_actor_id"].(string)
	return s, present
}

func TestActorPinning(t *testing.T) {
	h := newHarnessWith(t, testToken, nil, tools.WithActorID("act_pinned"))

	// The model supplies a different sender: it is ignored, not an error.
	r := h.ok(t, tools.ToolMessageAppend, map[string]any{"thread_id": "t1", "role": "agent", "content": "hi", "sender_actor_id": "act_spoofed"})
	if s, _ := storedSender(t, h, "msg/t1/"+r["id"].(string)); s != "act_pinned" {
		t.Errorf("stored sender = %q, want act_pinned", s)
	}
	// No sender supplied: the pinned one is used.
	r = h.ok(t, tools.ToolMessageAppend, map[string]any{"thread_id": "t1", "role": "agent", "content": "hi"})
	if s, _ := storedSender(t, h, "msg/t1/"+r["id"].(string)); s != "act_pinned" {
		t.Errorf("stored sender = %q, want act_pinned", s)
	}

	schema := toolSchema(t, h, tools.ToolMessageAppend)
	if d, _ := prop(t, schema, "sender_actor_id")["description"].(string); !strings.Contains(d, "Ignored") {
		t.Errorf("pinned schema should say the argument is ignored, got %q", d)
	}
	res, _ := h.session.ListTools(context.Background(), nil)
	for _, tl := range res.Tools {
		if tl.Name == tools.ToolMessageAppend && !strings.Contains(tl.Description, "act_pinned") {
			t.Errorf("description should name the pinned actor: %s", tl.Description)
		}
	}
}

func TestActorNotPinnedByDefault(t *testing.T) {
	h := newHarness(t, testToken)
	r := h.ok(t, tools.ToolMessageAppend, map[string]any{"thread_id": "t1", "role": "agent", "content": "hi", "sender_actor_id": "act_model"})
	if s, _ := storedSender(t, h, "msg/t1/"+r["id"].(string)); s != "act_model" {
		t.Errorf("stored sender = %q, want the supplied one", s)
	}
	r = h.ok(t, tools.ToolMessageAppend, map[string]any{"thread_id": "t1", "role": "agent", "content": "hi"})
	if _, present := storedSender(t, h, "msg/t1/"+r["id"].(string)); present {
		t.Error("no sender should be stored when none is supplied or pinned")
	}
}

func TestContentCapCountsEscapedBytes(t *testing.T) {
	h := newHarness(t, testToken)
	// 50,000 control characters are 50,000 bytes raw but six times that
	// once JSON-escaped, which is what is listed back.
	h.fail(t, tools.ToolMessageAppend, map[string]any{"thread_id": "t1", "role": "user", "content": strings.Repeat("\x01", 50000)}, "content", "escaping")
	// So do < > & (Go escapes them as \u003c and so on).
	h.fail(t, tools.ToolMessageAppend, map[string]any{"thread_id": "t1", "role": "user", "content": strings.Repeat("<", 50000)}, "content", "escaping")
	// The largest content that still fits: 43690 control chars = 262140 bytes escaped.
	h.ok(t, tools.ToolMessageAppend, map[string]any{"thread_id": "t1", "role": "user", "content": strings.Repeat("\x01", 43690)})
	if n := len(h.fake.objects); n != 1 {
		t.Errorf("fake stored %d objects, want 1", n)
	}
}

// A worst-case thread: messages written by other clients (no input cap),
// each full of characters that expand six-fold when escaped. The list result
// stays bounded and tells the model what was withheld.
func TestListBoundedWithEscapedAndOversizedMessages(t *testing.T) {
	h := newHarness(t, testToken)
	put := func(i int, content string) {
		h.fake.mu.Lock()
		defer h.fake.mu.Unlock()
		h.fake.nextID++
		id := fmt.Sprintf("msg_%04d", h.fake.nextID)
		data, _ := json.Marshal(map[string]any{"id": id, "thread_id": "t1", "role": "user", "content": content, "created_at": fmt.Sprintf("2026-01-01T00:00:%02dZ", i)})
		h.fake.objects["msg/t1/"+id] = obj{data: data, rev: 1}
	}
	put(1, strings.Repeat("\x01", 200000)) // 1.2 MB escaped: oversized on its own
	for i := 2; i <= 30; i++ {
		put(i, strings.Repeat("<", 8000)) // 48 KB escaped each
	}

	res := h.call(t, tools.ToolMessageList, map[string]any{"thread_id": "t1", "limit": 30})
	if res.IsError {
		t.Fatalf("list failed: %s", text(res))
	}
	out := h.ok(t, tools.ToolMessageList, map[string]any{"thread_id": "t1", "limit": 30})

	oversized, _ := out["oversized_message_ids"].([]any)
	if len(oversized) != 1 || oversized[0] != "msg_0001" {
		t.Errorf("oversized ids = %v", out["oversized_message_ids"])
	}
	msgs := out["messages"].([]any)
	first := msgs[0].(map[string]any)
	if first["id"] != "msg_0001" || first["content"] != "" {
		t.Errorf("first message should be a content-less stub: %v", first)
	}
	if out["truncated"] != true || out["has_more"] != true {
		t.Errorf("expected truncated and has_more: truncated=%v has_more=%v", out["truncated"], out["has_more"])
	}
	if !strings.Contains(out["hint"].(string), "oversized_message_ids") {
		t.Errorf("hint should mention oversized messages: %v", out["hint"])
	}

	// The whole result on the wire (structured copy plus the text copy)
	// stays well under 1 MiB.
	structured, _ := json.Marshal(res.StructuredContent)
	if total := len(structured) + len(text(res)); total > 1<<20 {
		t.Errorf("worst-case list result is %d bytes", total)
	}
	// Paging past it works.
	next := h.ok(t, tools.ToolMessageList, map[string]any{"thread_id": "t1", "limit": 30, "after_id": out["last_id"]})
	if len(next["messages"].([]any)) == 0 {
		t.Error("expected more messages after last_id")
	}
}

func TestReadOnlyHasNoWriteSchemas(t *testing.T) {
	h := newHarnessWith(t, testToken, nil, tools.ReadOnly(), tools.WithActorID("act_x"))
	res, _ := h.session.ListTools(context.Background(), nil)
	for _, tl := range res.Tools {
		if tl.Name == tools.ToolMessageAppend {
			t.Error("read-only server registered sync_message_append")
		}
	}
}
