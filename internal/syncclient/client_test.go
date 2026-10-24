package syncclient

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"unicode/utf8"
)

func newTestClient(t *testing.T, h http.Handler, mutate ...func(*Config)) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	cfg := Config{BaseURL: srv.URL, APIKey: "secret-token", Org: "org1", Project: "proj1"}
	for _, m := range mutate {
		m(&cfg)
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestNewValidation(t *testing.T) {
	good := Config{BaseURL: "http://localhost:8080", APIKey: "k", Org: "o", Project: "p"}
	if _, err := New(good); err != nil {
		t.Fatalf("good config rejected: %v", err)
	}
	cases := map[string]func(*Config){
		"empty org":       func(c *Config) { c.Org = "" },
		"bad org":         func(c *Config) { c.Org = "a/b" },
		"empty project":   func(c *Config) { c.Project = "" },
		"dotdot project":  func(c *Config) { c.Project = ".." },
		"empty key":       func(c *Config) { c.APIKey = "  " },
		"no scheme":       func(c *Config) { c.BaseURL = "localhost:8080" },
		"ftp scheme":      func(c *Config) { c.BaseURL = "ftp://x" },
		"url credentials": func(c *Config) { c.BaseURL = "http://u:p@host" },
		"url query":       func(c *Config) { c.BaseURL = "http://host?x=1" },
	}
	for name, mutate := range cases {
		cfg := good
		mutate(&cfg)
		if _, err := New(cfg); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestValidateID(t *testing.T) {
	for _, id := range []string{"a", "A-b_c.1", "job.2"} {
		if err := ValidateID("x", id); err != nil {
			t.Errorf("%q should be valid: %v", id, err)
		}
	}
	for _, id := range []string{"", ".", "..", "a/b", "a b", "a%2fb", "é", "a?b", "a#b"} {
		if err := ValidateID("x", id); err == nil {
			t.Errorf("%q should be invalid", id)
		}
	}
}

func TestAuthHeaderAndTenantPath(t *testing.T) {
	var gotPath, gotAuth string
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotAuth = r.URL.EscapedPath(), r.Header.Get("Authorization")
		w.Header().Set("ETag", `"abc"`)
		_, _ = w.Write([]byte(`{"id":"s1","status":"open","thread_id":"t1"}`))
	}))
	s, etag, err := c.GetSession(context.Background(), "s1")
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/v1/orgs/org1/projects/proj1/sessions/s1" {
		t.Errorf("path = %q", gotPath)
	}
	if gotAuth != "Bearer secret-token" {
		t.Errorf("auth = %q", gotAuth)
	}
	if etag != "abc" || s.ID != "s1" {
		t.Errorf("etag=%q session=%+v", etag, s)
	}
}

func TestHealthSendsNoKey(t *testing.T) {
	var auth string
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	out, err := c.Health(context.Background())
	if err != nil || out["status"] != "ok" {
		t.Fatalf("out=%v err=%v", out, err)
	}
	if auth != "" {
		t.Errorf("health request carried Authorization header %q", auth)
	}
}

func TestBasePathPrefix(t *testing.T) {
	var gotPath string
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}), func(c *Config) { c.BaseURL += "/sync/" })
	if _, err := c.Health(context.Background()); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/sync/healthz" {
		t.Errorf("path = %q", gotPath)
	}
}

func TestInvalidIDsRejectedBeforeRequest(t *testing.T) {
	var calls atomic.Int32
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	ctx := context.Background()
	if _, _, err := c.GetSession(ctx, ""); err == nil {
		t.Error("empty session id accepted")
	}
	if _, _, err := c.GetSession(ctx, "../x"); err == nil {
		t.Error("traversal session id accepted")
	}
	if _, err := c.ListMessages(ctx, "a/b", "", 0); err == nil {
		t.Error("slash thread id accepted")
	}
	if _, err := c.ListMessages(ctx, "t", "bad id", 0); err == nil {
		t.Error("bad after_id accepted")
	}
	if _, _, err := c.GetJob(ctx, ""); err == nil {
		t.Error("empty job id accepted")
	}
	if _, _, err := c.ClaimJob(ctx, "j", ""); err == nil {
		t.Error("empty worker id accepted")
	}
	if calls.Load() != 0 {
		t.Errorf("%d requests reached the server", calls.Load())
	}
}

func TestIfMatchQuoting(t *testing.T) {
	var got []string
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.Header.Get("If-Match"))
		w.Header().Set("ETag", `"new"`)
		_, _ = w.Write([]byte(`{"id":"s","status":"open","thread_id":"t"}`))
	}))
	ctx := context.Background()
	for _, in := range []string{"", "abc", `"abc"`} {
		if _, _, err := c.PutSession(ctx, "s", Session{Status: "open", ThreadID: "t"}, in); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"", `"abc"`, `"abc"`}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("If-Match[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestAPIErrorAndKeyRedaction(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"echoing secret-token back"}`))
	}))
	_, _, err := c.GetJob(context.Background(), "j1")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != http.StatusConflict {
		t.Fatalf("err = %v", err)
	}
	if strings.Contains(err.Error(), "secret-token") {
		t.Errorf("error leaks key: %v", err)
	}
}

func TestPlainTextErrorBody(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Unauthorized: invalid API key", http.StatusUnauthorized)
	}))
	_, _, err := c.GetJob(context.Background(), "j1")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 401 || !strings.Contains(apiErr.Message, "invalid API key") {
		t.Fatalf("err = %v", err)
	}
}

func TestRedirectsNotFollowed(t *testing.T) {
	var targetHit atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHit.Store(true)
	}))
	t.Cleanup(target.Close)
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	_, _, err := c.GetJob(context.Background(), "j1")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != http.StatusTemporaryRedirect {
		t.Fatalf("err = %v", err)
	}
	if targetHit.Load() {
		t.Error("redirect was followed")
	}
}

func TestConnectionErrorDoesNotLeakKey(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	c, err := New(Config{BaseURL: url, APIKey: "secret-token", Org: "o", Project: "p"})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = c.GetJob(context.Background(), "j")
	if err == nil || strings.Contains(err.Error(), "secret-token") {
		t.Fatalf("err = %v", err)
	}
}

func TestAPIKeyIsTrimmed(t *testing.T) {
	var got string
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"id":"j1"}`))
	}), func(cfg *Config) { cfg.APIKey = "  secret-token\r\n" })
	if _, _, err := c.GetJob(context.Background(), "j1"); err != nil {
		t.Fatalf("key with trailing newline should work: %v", err)
	}
	if got != "Bearer secret-token" {
		t.Errorf("Authorization = %q", got)
	}
}

func TestOversizeSuccessResponseIsExplicitError(t *testing.T) {
	body := `{"messages":[{"id":"m1","content":"` + strings.Repeat("z", 5000) + `"}]}`
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	}), func(cfg *Config) { cfg.MaxResponseBytes = 1000 })
	_, err := c.ListMessages(context.Background(), "t1", "", 0)
	var tooBig *ResponseTooLargeError
	if !errors.As(err, &tooBig) || tooBig.Limit != 1000 {
		t.Fatalf("err = %v", err)
	}
	if !strings.Contains(err.Error(), "response exceeds 1000 bytes; use a smaller limit") || strings.Contains(err.Error(), "decoding") {
		t.Errorf("unclear error: %v", err)
	}
}

func TestResponseExactlyAtLimitIsAccepted(t *testing.T) {
	body := `{"id":"j1","kind":"custom"}`
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	}), func(cfg *Config) { cfg.MaxResponseBytes = int64(len(body)) })
	if _, _, err := c.GetJob(context.Background(), "j1"); err != nil {
		t.Fatalf("response at the limit should be accepted: %v", err)
	}
}

func TestOversizeErrorBodyStillAnAPIError(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(strings.Repeat("e", 5000)))
	}), func(cfg *Config) { cfg.MaxResponseBytes = 1000 })
	_, _, err := c.GetJob(context.Background(), "j1")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != http.StatusBadGateway || len(apiErr.Message) > 600 {
		t.Fatalf("err = %v", err)
	}
}

func TestErrorTextRedactsBeforeTruncatingOnRuneBoundary(t *testing.T) {
	// The key straddles the 500-byte cut: truncating first would leave a
	// recognizable prefix of it in the message.
	key := "secret-token"
	msg := strings.Repeat("a", 495) + key + strings.Repeat("b", 50)
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(msg))
	}))
	_, _, err := c.GetJob(context.Background(), "j1")
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatal(err)
	}
	if strings.Contains(apiErr.Message, "secret") || strings.Contains(apiErr.Message, "token") {
		t.Errorf("message leaks part of the key: %q", apiErr.Message)
	}

	// Multi-byte characters are never split.
	c2 := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(strings.Repeat("é", 400))) // 800 bytes
	}))
	_, _, err = c2.GetJob(context.Background(), "j1")
	if !errors.As(err, &apiErr) || !utf8.ValidString(apiErr.Message) || !strings.HasSuffix(apiErr.Message, "...") {
		t.Errorf("message = %q", apiErr.Message)
	}
}
