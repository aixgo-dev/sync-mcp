package tools_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/aixgo-dev/sync-mcp/internal/syncclient"
	"github.com/aixgo-dev/sync-mcp/internal/tools"
)

const (
	testOrg     = "org1"
	testProject = "proj1"
	testToken   = "test-token"
)

// fakeSync is an in-memory stand-in for the Sync HTTP API that follows its
// documented behavior: bearer auth, tenant scoping, create-only PUT without
// If-Match, compare-and-swap with If-Match, and queued-to-claimed jobs.
type fakeSync struct {
	mu      sync.Mutex
	objects map[string]obj
	nextID  int
	forceCt map[string]int // path suffix -> status to force
}

type obj struct {
	data []byte
	rev  int
}

func (o obj) etag() string { return fmt.Sprintf("rev%d", o.rev) }

func newFake() *fakeSync { return &fakeSync{objects: map[string]obj{}, forceCt: map[string]int{}} }

func (f *fakeSync) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if r.URL.Path == "/healthz" {
		writeJSON(w, 200, `{"status":"ok"}`, "")
		return
	}
	if r.Header.Get("Authorization") != "Bearer "+testToken {
		http.Error(w, "Unauthorized: invalid API key", http.StatusUnauthorized)
		return
	}
	prefix := "/v1/orgs/" + testOrg + "/projects/" + testProject + "/"
	if !strings.HasPrefix(r.URL.Path, prefix) {
		http.Error(w, "Forbidden: tenant mismatch", http.StatusForbidden)
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, prefix)
	for suffix, code := range f.forceCt {
		if strings.HasSuffix(rest, suffix) {
			writeJSON(w, code, `{"error":"forced"}`, "")
			return
		}
	}
	parts := strings.Split(rest, "/")
	switch {
	case parts[0] == "sessions" && len(parts) == 2:
		f.session(w, r, parts[1])
	case parts[0] == "threads" && len(parts) == 3 && parts[2] == "messages":
		f.messages(w, r, parts[1])
	case parts[0] == "jobs" && len(parts) == 1 && r.Method == http.MethodPost:
		f.createJob(w, r)
	case parts[0] == "jobs" && len(parts) == 2 && r.Method == http.MethodGet:
		f.getJob(w, parts[1])
	case parts[0] == "jobs" && len(parts) == 3 && parts[2] == "claim" && r.Method == http.MethodPost:
		f.claimJob(w, r, parts[1])
	default:
		writeJSON(w, 404, `{"error":"no route"}`, "")
	}
}

func writeJSON(w http.ResponseWriter, code int, body, etag string) {
	w.Header().Set("Content-Type", "application/json")
	if etag != "" {
		w.Header().Set("ETag", `"`+etag+`"`)
	}
	w.WriteHeader(code)
	_, _ = w.Write([]byte(body))
}

func (f *fakeSync) session(w http.ResponseWriter, r *http.Request, id string) {
	key := "session/" + id
	switch r.Method {
	case http.MethodGet:
		o, ok := f.objects[key]
		if !ok {
			writeJSON(w, 404, `{"error":"Session not found"}`, "")
			return
		}
		writeJSON(w, 200, string(o.data), o.etag())
	case http.MethodPut:
		var s map[string]any
		if err := json.NewDecoder(r.Body).Decode(&s); err != nil {
			writeJSON(w, 400, `{"error":"Invalid JSON body"}`, "")
			return
		}
		s["id"] = id
		now := time.Now().UTC().Format(time.RFC3339Nano)
		if s["created_at"] == nil {
			s["created_at"] = now
		}
		s["updated_at"] = now
		data, _ := json.Marshal(s)
		cur, exists := f.objects[key]
		ifMatch := strings.Trim(r.Header.Get("If-Match"), `"`)
		switch {
		case ifMatch == "" && exists:
			writeJSON(w, 409, `{"error":"Session already exists"}`, "")
			return
		case ifMatch != "" && (!exists || ifMatch != cur.etag()):
			writeJSON(w, 412, `{"error":"Precondition failed (CAS mismatch)"}`, "")
			return
		}
		o := obj{data: data, rev: cur.rev + 1}
		f.objects[key] = o
		writeJSON(w, 200, string(data), o.etag())
	}
}

func (f *fakeSync) messages(w http.ResponseWriter, r *http.Request, thread string) {
	switch r.Method {
	case http.MethodPost:
		var in map[string]any
		_ = json.NewDecoder(r.Body).Decode(&in)
		f.nextID++
		id := fmt.Sprintf("msg_%04d", f.nextID)
		in["id"], in["thread_id"] = id, thread
		in["created_at"] = fmt.Sprintf("2026-01-01T00:00:%02dZ", f.nextID)
		data, _ := json.Marshal(in)
		f.objects["msg/"+thread+"/"+id] = obj{data: data, rev: 1}
		writeJSON(w, 201, string(data), "rev1")
	case http.MethodGet:
		after := r.URL.Query().Get("after_id")
		limit := 100
		if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 {
			limit = v
		}
		var msgs []json.RawMessage
		started := after == ""
		for i := 1; i <= f.nextID; i++ {
			id := fmt.Sprintf("msg_%04d", i)
			o, ok := f.objects["msg/"+thread+"/"+id]
			if !ok {
				continue
			}
			if !started {
				started = id == after
				continue
			}
			msgs = append(msgs, o.data)
		}
		if len(msgs) > limit {
			msgs = msgs[:limit]
		}
		out, _ := json.Marshal(map[string]any{"messages": msgs})
		writeJSON(w, 200, string(out), "")
	}
}

func (f *fakeSync) createJob(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Kind    string `json:"kind"`
		Payload any    `json:"payload"`
	}
	_ = json.NewDecoder(r.Body).Decode(&in)
	switch in.Kind {
	case "code_offload", "command", "schedule", "custom":
	default:
		writeJSON(w, 400, `{"error":"invalid job kind"}`, "")
		return
	}
	f.nextID++
	id := fmt.Sprintf("job%04d", f.nextID)
	job := map[string]any{"id": id, "org_id": testOrg, "project_id": testProject, "type": "job", "kind": in.Kind,
		"status": "queued", "payload": in.Payload, "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z"}
	data, _ := json.Marshal(job)
	f.objects["job/"+id] = obj{data: data, rev: 1}
	writeJSON(w, 201, string(data), "rev1")
}

func (f *fakeSync) getJob(w http.ResponseWriter, id string) {
	o, ok := f.objects["job/"+id]
	if !ok {
		writeJSON(w, 404, `{"error":"job not found"}`, "")
		return
	}
	writeJSON(w, 200, string(o.data), o.etag())
}

func (f *fakeSync) claimJob(w http.ResponseWriter, r *http.Request, id string) {
	var in struct {
		WorkerID string `json:"worker_id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&in)
	if in.WorkerID == "" {
		writeJSON(w, 400, `{"error":"worker_id is required"}`, "")
		return
	}
	o, ok := f.objects["job/"+id]
	if !ok {
		writeJSON(w, 404, `{"error":"job not found"}`, "")
		return
	}
	var job map[string]any
	_ = json.Unmarshal(o.data, &job)
	if job["status"] != "queued" {
		writeJSON(w, 409, `{"error":"job is already claimed or not in queued state"}`, "")
		return
	}
	job["status"], job["assignee_worker"] = "claimed", in.WorkerID
	data, _ := json.Marshal(job)
	n := obj{data: data, rev: o.rev + 1}
	f.objects["job/"+id] = n
	writeJSON(w, 200, string(data), n.etag())
}

// harness wires a fake Sync server, a real client and an MCP server/client
// pair over in-memory transports.
type harness struct {
	fake    *fakeSync
	session *mcp.ClientSession
}

func newHarness(t *testing.T, token string) *harness {
	t.Helper()
	return newHarnessWith(t, token, nil)
}

// newHarnessWith is newHarness with a client config tweak and register options.
func newHarnessWith(t *testing.T, token string, mutate func(*syncclient.Config), opts ...tools.Option) *harness {
	t.Helper()
	fake := newFake()
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)

	cfg := syncclient.Config{BaseURL: srv.URL, APIKey: token, Org: testOrg, Project: testProject}
	if mutate != nil {
		mutate(&cfg)
	}
	client, err := syncclient.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server := mcp.NewServer(&mcp.Implementation{Name: "sync-mcp", Version: "test"}, nil)
	tools.Register(server, client, opts...)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	st, ct := mcp.NewInMemoryTransports()
	ss, err := server.Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "test"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return &harness{fake: fake, session: cs}
}

func (h *harness) call(t *testing.T, name string, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	res, err := h.session.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("%s: protocol error: %v", name, err)
	}
	return res
}

func (h *harness) ok(t *testing.T, name string, args map[string]any) map[string]any {
	t.Helper()
	res := h.call(t, name, args)
	if res.IsError {
		t.Fatalf("%s: unexpected tool error: %s", name, text(res))
	}
	m, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("%s: structured content is %T", name, res.StructuredContent)
	}
	return m
}

func (h *harness) fail(t *testing.T, name string, args map[string]any, want ...string) {
	t.Helper()
	res := h.call(t, name, args)
	if !res.IsError {
		t.Fatalf("%s: expected isError result, got %s", name, text(res))
	}
	msg := text(res)
	for _, w := range want {
		if !strings.Contains(msg, w) {
			t.Errorf("%s: error %q does not contain %q", name, msg, w)
		}
	}
	if strings.Contains(msg, testToken) {
		t.Errorf("%s: error leaks API key: %q", name, msg)
	}
}

func text(res *mcp.CallToolResult) string {
	var b strings.Builder
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			b.WriteString(tc.Text)
		}
	}
	return b.String()
}

func sub(t *testing.T, m map[string]any, key string) map[string]any {
	t.Helper()
	v, ok := m[key].(map[string]any)
	if !ok {
		t.Fatalf("%q is %T in %v", key, m[key], m)
	}
	return v
}

func TestToolsList(t *testing.T) {
	h := newHarness(t, testToken)
	res, err := h.session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		tools.ToolHealth: false, tools.ToolSessionRead: false, tools.ToolSessionWrite: false,
		tools.ToolMessageAppend: false, tools.ToolMessageList: false,
		tools.ToolJobCreate: false, tools.ToolJobGet: false, tools.ToolJobClaim: false,
	}
	for _, tl := range res.Tools {
		if _, ok := want[tl.Name]; !ok {
			t.Errorf("unexpected tool %q", tl.Name)
		}
		want[tl.Name] = true
		if tl.Description == "" || tl.InputSchema == nil {
			t.Errorf("tool %q lacks description or input schema", tl.Name)
		}
		// Tenant selection must never be an argument.
		schema, _ := json.Marshal(tl.InputSchema)
		for _, forbidden := range []string{`"org"`, `"project"`, `"org_id"`, `"project_id"`, `"api_key"`} {
			if strings.Contains(string(schema), forbidden) {
				t.Errorf("tool %q exposes %s in its input schema", tl.Name, forbidden)
			}
		}
	}
	for name, seen := range want {
		if !seen {
			t.Errorf("missing tool %q", name)
		}
	}
}

func TestHealth(t *testing.T) {
	h := newHarness(t, testToken)
	if got := h.ok(t, tools.ToolHealth, nil)["status"]; got != "ok" {
		t.Errorf("status = %v", got)
	}
}

func TestHealthWorksWithBadKey(t *testing.T) {
	h := newHarness(t, "wrong")
	if got := h.ok(t, tools.ToolHealth, nil)["status"]; got != "ok" {
		t.Errorf("status = %v", got)
	}
}

func TestSessionCreateReadUpdateETagRoundTrip(t *testing.T) {
	h := newHarness(t, testToken)

	created := h.ok(t, tools.ToolSessionWrite, map[string]any{
		"session_id": "s1", "thread_id": "t1", "labels": map[string]any{"team": "a"},
	})
	etag1 := created["etag"].(string)
	if etag1 == "" {
		t.Fatal("create returned no etag")
	}
	if sub(t, created, "session")["status"] != "open" {
		t.Errorf("status should default to open: %v", created)
	}

	read := h.ok(t, tools.ToolSessionRead, map[string]any{"session_id": "s1"})
	if read["etag"] != etag1 {
		t.Errorf("read etag %v != create etag %v", read["etag"], etag1)
	}
	sess := sub(t, read, "session")
	if sess["thread_id"] != "t1" || sub(t, sess, "labels")["team"] != "a" {
		t.Errorf("unexpected session %v", sess)
	}

	updated := h.ok(t, tools.ToolSessionWrite, map[string]any{
		"session_id": "s1", "thread_id": "t1", "status": "closed", "worker_id": "w1",
		"created_at": sess["created_at"], "if_match": etag1,
	})
	etag2 := updated["etag"].(string)
	if etag2 == etag1 {
		t.Error("etag did not change after update")
	}
	us := sub(t, updated, "session")
	if us["status"] != "closed" || us["worker_id"] != "w1" || us["created_at"] != sess["created_at"] {
		t.Errorf("unexpected updated session %v", us)
	}

	// Quoted ETags (as seen in HTTP headers) are accepted too.
	h.ok(t, tools.ToolSessionWrite, map[string]any{
		"session_id": "s1", "thread_id": "t1", "if_match": `"` + etag2 + `"`,
	})
}

func TestSessionConflictAndPreconditionFailed(t *testing.T) {
	h := newHarness(t, testToken)
	first := h.ok(t, tools.ToolSessionWrite, map[string]any{"session_id": "s1", "thread_id": "t1"})
	etag := first["etag"].(string)

	// Create-only over an existing session -> 409.
	h.fail(t, tools.ToolSessionWrite, map[string]any{"session_id": "s1", "thread_id": "t1"}, "409", "already exists")

	// Update with the current etag succeeds, then the old etag is stale -> 412.
	h.ok(t, tools.ToolSessionWrite, map[string]any{"session_id": "s1", "thread_id": "t1", "if_match": etag})
	h.fail(t, tools.ToolSessionWrite, map[string]any{"session_id": "s1", "thread_id": "t1", "if_match": etag}, "412", "Re-read")

	// Updating a session that does not exist with an etag -> 412.
	h.fail(t, tools.ToolSessionWrite, map[string]any{"session_id": "nope", "thread_id": "t1", "if_match": "rev1"}, "412")
}

func TestSessionNotFoundAndValidation(t *testing.T) {
	h := newHarness(t, testToken)
	h.fail(t, tools.ToolSessionRead, map[string]any{"session_id": "missing"}, "404")
	h.fail(t, tools.ToolSessionRead, map[string]any{"session_id": ""}, "session_id")
	h.fail(t, tools.ToolSessionRead, map[string]any{"session_id": "../other"}, "session_id")
	h.fail(t, tools.ToolSessionWrite, map[string]any{"session_id": "s", "thread_id": "t", "status": "weird"}, "status")
	h.fail(t, tools.ToolSessionWrite, map[string]any{"session_id": "s", "thread_id": ""}, "thread_id")
	h.fail(t, tools.ToolSessionWrite, map[string]any{"session_id": "", "thread_id": "t"}, "session_id")
}

func TestMessageAppendAndList(t *testing.T) {
	h := newHarness(t, testToken)
	var ids []string
	for i := 0; i < 3; i++ {
		out := h.ok(t, tools.ToolMessageAppend, map[string]any{
			"thread_id": "t1", "role": "agent", "content": fmt.Sprintf("hello %d", i),
			"sender_actor_id": "actor-1", "refs": map[string]any{"n": i},
		})
		if out["id"] == "" || out["thread_id"] != "t1" || out["created_at"] == "" || out["etag"] == "" {
			t.Fatalf("incomplete result %v", out)
		}
		ids = append(ids, out["id"].(string))
	}
	h.ok(t, tools.ToolMessageAppend, map[string]any{"thread_id": "other", "role": "user", "content": "elsewhere"})

	list := h.ok(t, tools.ToolMessageList, map[string]any{"thread_id": "t1"})
	msgs := list["messages"].([]any)
	if len(msgs) != 3 || list["count"].(float64) != 3 || list["last_id"] != ids[2] || list["has_more"] != false || list["truncated"] != false {
		t.Fatalf("unexpected list %v", list)
	}
	if msgs[0].(map[string]any)["content"] != "hello 0" {
		t.Errorf("unexpected order: %v", msgs)
	}

	page := h.ok(t, tools.ToolMessageList, map[string]any{"thread_id": "t1", "after_id": ids[0], "limit": 1})
	pm := page["messages"].([]any)
	if len(pm) != 1 || pm[0].(map[string]any)["id"] != ids[1] || page["last_id"] != ids[1] {
		t.Errorf("unexpected page %v", page)
	}

	empty := h.ok(t, tools.ToolMessageList, map[string]any{"thread_id": "unused"})
	if len(empty["messages"].([]any)) != 0 || empty["count"].(float64) != 0 {
		t.Errorf("expected empty list, got %v", empty)
	}
}

func TestMessageValidation(t *testing.T) {
	h := newHarness(t, testToken)
	h.fail(t, tools.ToolMessageAppend, map[string]any{"thread_id": "t", "role": "robot", "content": "x"}, "role")
	h.fail(t, tools.ToolMessageAppend, map[string]any{"thread_id": "", "role": "user", "content": "x"}, "thread_id")
	h.fail(t, tools.ToolMessageAppend, map[string]any{"thread_id": "a/b", "role": "user", "content": "x"}, "thread_id")
	h.fail(t, tools.ToolMessageList, map[string]any{"thread_id": ""}, "thread_id")
	h.fail(t, tools.ToolMessageList, map[string]any{"thread_id": "t", "limit": 5000}, "limit")
	h.fail(t, tools.ToolMessageList, map[string]any{"thread_id": "t", "limit": -1}, "limit")
}

func TestJobLifecycle(t *testing.T) {
	h := newHarness(t, testToken)
	created := h.ok(t, tools.ToolJobCreate, map[string]any{"kind": "command", "payload": map[string]any{"cmd": "echo hi"}})
	job := sub(t, created, "job")
	id := job["id"].(string)
	if job["status"] != "queued" || created["etag"] == "" {
		t.Fatalf("unexpected created job %v", created)
	}
	if sub(t, job, "payload")["cmd"] != "echo hi" {
		t.Errorf("payload lost: %v", job)
	}

	got := h.ok(t, tools.ToolJobGet, map[string]any{"job_id": id})
	if got["etag"] != created["etag"] || sub(t, got, "job")["status"] != "queued" {
		t.Errorf("unexpected get %v", got)
	}

	claimed := h.ok(t, tools.ToolJobClaim, map[string]any{"job_id": id, "worker_id": "w1"})
	cj := sub(t, claimed, "job")
	if cj["status"] != "claimed" || cj["assignee_worker"] != "w1" || claimed["etag"] == created["etag"] {
		t.Errorf("unexpected claim %v", claimed)
	}

	// A second claim loses.
	h.fail(t, tools.ToolJobClaim, map[string]any{"job_id": id, "worker_id": "w2"}, "409", "already claimed")
}

func TestJobErrors(t *testing.T) {
	h := newHarness(t, testToken)
	h.fail(t, tools.ToolJobCreate, map[string]any{"kind": "bogus"}, "kind")
	h.fail(t, tools.ToolJobGet, map[string]any{"job_id": "missing"}, "404")
	h.fail(t, tools.ToolJobGet, map[string]any{"job_id": ""}, "job_id")
	h.fail(t, tools.ToolJobClaim, map[string]any{"job_id": "missing", "worker_id": "w"}, "404")
	h.fail(t, tools.ToolJobClaim, map[string]any{"job_id": "x", "worker_id": ""}, "worker_id")
}

func TestJobClaimPreconditionFailed(t *testing.T) {
	h := newHarness(t, testToken)
	id := sub(t, h.ok(t, tools.ToolJobCreate, map[string]any{"kind": "custom"}), "job")["id"].(string)
	h.fake.forceCt[id+"/claim"] = http.StatusPreconditionFailed
	h.fail(t, tools.ToolJobClaim, map[string]any{"job_id": id, "worker_id": "w"}, "412", "Re-read")
}

func TestAuthFailureIsToolError(t *testing.T) {
	h := newHarness(t, "wrong-token")
	calls := map[string]map[string]any{
		tools.ToolSessionRead:   {"session_id": "s"},
		tools.ToolSessionWrite:  {"session_id": "s", "thread_id": "t"},
		tools.ToolMessageAppend: {"thread_id": "t", "role": "user", "content": "x"},
		tools.ToolMessageList:   {"thread_id": "t"},
		tools.ToolJobCreate:     {"kind": "custom"},
		tools.ToolJobGet:        {"job_id": "j"},
		tools.ToolJobClaim:      {"job_id": "j", "worker_id": "w"},
	}
	for name, args := range calls {
		h.fail(t, name, args, "401", "API key")
	}
}

func TestForbiddenAndServerErrors(t *testing.T) {
	h := newHarness(t, testToken)
	h.fake.forceCt["sessions/s403"] = http.StatusForbidden
	h.fail(t, tools.ToolSessionRead, map[string]any{"session_id": "s403"}, "403", "not allowed")

	h.fake.forceCt["sessions/s500"] = http.StatusInternalServerError
	h.fail(t, tools.ToolSessionRead, map[string]any{"session_id": "s500"}, "500", "forced")

	// The server stays usable after tool errors.
	if got := h.ok(t, tools.ToolHealth, nil)["status"]; got != "ok" {
		t.Errorf("status = %v", got)
	}
}

func TestUnreachableServerIsToolError(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	client, err := syncclient.New(syncclient.Config{BaseURL: url, APIKey: testToken, Org: testOrg, Project: testProject})
	if err != nil {
		t.Fatal(err)
	}
	server := mcp.NewServer(&mcp.Implementation{Name: "s", Version: "t"}, nil)
	tools.Register(server, client)
	ctx := context.Background()
	st, ct := mcp.NewInMemoryTransports()
	ss, err := server.Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "c", Version: "t"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: tools.ToolHealth})
	if err != nil {
		t.Fatalf("protocol error: %v", err)
	}
	if !res.IsError || strings.Contains(text(res), testToken) {
		t.Errorf("unexpected result: isError=%v %s", res.IsError, text(res))
	}
}
