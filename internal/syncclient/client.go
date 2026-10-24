// Package syncclient is a small HTTP client for the Sync coordination API.
//
// A Client is bound to a single organization and project for its whole
// lifetime. Callers can pick resource IDs (session, thread, job) but never the
// tenant, which keeps every request inside the scope the client was
// configured with.
package syncclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	// DefaultTimeout is the per-request timeout used when none is configured.
	DefaultTimeout = 15 * time.Second

	// maxResponseBytes caps how much of a response body is read.
	maxResponseBytes = 8 << 20
)

// Config configures a Client.
type Config struct {
	// BaseURL is the root URL of the Sync server, e.g. http://localhost:8080.
	BaseURL string
	// APIKey is the bearer token sent on every /v1 request. It is never
	// included in errors or logs.
	APIKey string
	// Org and Project are the tenant every request is scoped to.
	Org     string
	Project string
	// Timeout is the per-request timeout. Zero means DefaultTimeout.
	Timeout time.Duration
	// HTTPClient overrides the underlying HTTP client (used in tests).
	HTTPClient *http.Client
}

// Client talks to one Sync server on behalf of one org and project.
type Client struct {
	base    *url.URL
	apiKey  string
	org     string
	project string
	http    *http.Client
}

// APIError is returned when Sync answers with a non-2xx status.
type APIError struct {
	Status  int
	Message string
}

func (e *APIError) Error() string {
	text := http.StatusText(e.Status)
	if e.Message == "" {
		return fmt.Sprintf("sync returned %d %s", e.Status, text)
	}
	return fmt.Sprintf("sync returned %d %s: %s", e.Status, text, e.Message)
}

// New validates cfg and returns a Client.
func New(cfg Config) (*Client, error) {
	if err := ValidateID("org", cfg.Org); err != nil {
		return nil, err
	}
	if err := ValidateID("project", cfg.Project); err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.APIKey) == "" {
		return nil, errors.New("api key must not be empty")
	}
	u, err := url.Parse(strings.TrimSpace(cfg.BaseURL))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errors.New("base URL must be an absolute http or https URL")
	}
	if u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return nil, errors.New("base URL must not contain credentials, a query or a fragment")
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawPath = ""

	hc := cfg.HTTPClient
	if hc == nil {
		timeout := cfg.Timeout
		if timeout <= 0 {
			timeout = DefaultTimeout
		}
		hc = &http.Client{Timeout: timeout}
	}
	// Never follow redirects: that could forward the bearer token elsewhere.
	hcCopy := *hc
	hcCopy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	return &Client{
		base:    u,
		apiKey:  cfg.APIKey,
		org:     cfg.Org,
		project: cfg.Project,
		http:    &hcCopy,
	}, nil
}

// ValidateID checks that a path segment is non-empty and uses only the
// characters Sync accepts for identifiers: letters, digits, '.', '_' and '-'
// (and is not "." or "..").
func ValidateID(name, id string) error {
	if id == "" {
		return fmt.Errorf("%s must not be empty", name)
	}
	if id == "." || id == ".." {
		return fmt.Errorf("%s %q is not a valid identifier", name, id)
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		ok := (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-'
		if !ok {
			return fmt.Errorf("%s may only contain letters, digits, '.', '_' and '-'", name)
		}
	}
	return nil
}

// Session is a coordination session document.
type Session struct {
	ID        string            `json:"id,omitempty"`
	Status    string            `json:"status"`
	WorkerID  string            `json:"worker_id,omitempty"`
	ThreadID  string            `json:"thread_id"`
	Labels    map[string]string `json:"labels,omitempty"`
	CreatedAt string            `json:"created_at,omitempty"`
	UpdatedAt string            `json:"updated_at,omitempty"`
}

// Message is a message in a thread.
type Message struct {
	ID            string         `json:"id"`
	ThreadID      string         `json:"thread_id"`
	SenderActorID string         `json:"sender_actor_id,omitempty"`
	Role          string         `json:"role"`
	Content       string         `json:"content"`
	Refs          map[string]any `json:"refs,omitempty"`
	CreatedAt     string         `json:"created_at"`
}

// NewMessage is the body of a message append.
type NewMessage struct {
	SenderActorID string         `json:"sender_actor_id,omitempty"`
	Role          string         `json:"role"`
	Content       string         `json:"content"`
	Refs          map[string]any `json:"refs,omitempty"`
}

// Job is a unit of queued work.
type Job struct {
	ID             string `json:"id"`
	OrgID          string `json:"org_id,omitempty"`
	ProjectID      string `json:"project_id,omitempty"`
	Type           string `json:"type,omitempty"`
	Kind           string `json:"kind"`
	Status         string `json:"status"`
	Payload        any    `json:"payload,omitempty"`
	AssigneeWorker string `json:"assignee_worker,omitempty"`
	Result         any    `json:"result,omitempty"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
}

// Health reports the server's /healthz status.
func (c *Client) Health(ctx context.Context) (map[string]any, error) {
	var out map[string]any
	// /healthz is the only unauthenticated route; no key is sent.
	if _, err := c.do(ctx, http.MethodGet, "/healthz", nil, nil, &out, false); err != nil {
		return nil, err
	}
	return out, nil
}

// GetSession reads a session and returns it with its ETag.
func (c *Client) GetSession(ctx context.Context, id string) (Session, string, error) {
	if err := ValidateID("session_id", id); err != nil {
		return Session{}, "", err
	}
	var s Session
	etag, err := c.do(ctx, http.MethodGet, c.tenantPath("sessions", id), nil, nil, &s, true)
	return s, etag, err
}

// PutSession creates (ifMatch == "") or updates (ifMatch == a previously seen
// ETag) a session and returns the stored document with its new ETag.
func (c *Client) PutSession(ctx context.Context, id string, s Session, ifMatch string) (Session, string, error) {
	if err := ValidateID("session_id", id); err != nil {
		return Session{}, "", err
	}
	hdr := http.Header{}
	if ifMatch != "" {
		hdr.Set("If-Match", quoteETag(ifMatch))
	}
	var out Session
	etag, err := c.do(ctx, http.MethodPut, c.tenantPath("sessions", id), s, hdr, &out, true)
	return out, etag, err
}

// AppendMessage appends a message to a thread.
func (c *Client) AppendMessage(ctx context.Context, threadID string, m NewMessage) (Message, string, error) {
	if err := ValidateID("thread_id", threadID); err != nil {
		return Message{}, "", err
	}
	var out Message
	etag, err := c.do(ctx, http.MethodPost, c.tenantPath("threads", threadID, "messages"), m, nil, &out, true)
	return out, etag, err
}

// ListMessages lists messages in a thread in stable order. afterID and limit
// are optional (limit <= 0 uses the server default).
func (c *Client) ListMessages(ctx context.Context, threadID, afterID string, limit int) ([]Message, error) {
	if err := ValidateID("thread_id", threadID); err != nil {
		return nil, err
	}
	q := url.Values{}
	if afterID != "" {
		if err := ValidateID("after_id", afterID); err != nil {
			return nil, err
		}
		q.Set("after_id", afterID)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	p := c.tenantPath("threads", threadID, "messages")
	if len(q) > 0 {
		p += "?" + q.Encode()
	}
	var out struct {
		Messages []Message `json:"messages"`
	}
	if _, err := c.do(ctx, http.MethodGet, p, nil, nil, &out, true); err != nil {
		return nil, err
	}
	if out.Messages == nil {
		out.Messages = []Message{}
	}
	return out.Messages, nil
}

// CreateJob enqueues a job and returns it with its ETag.
func (c *Client) CreateJob(ctx context.Context, kind string, payload any) (Job, string, error) {
	body := struct {
		Kind    string `json:"kind"`
		Payload any    `json:"payload,omitempty"`
	}{kind, payload}
	var out Job
	etag, err := c.do(ctx, http.MethodPost, c.tenantPath("jobs"), body, nil, &out, true)
	return out, etag, err
}

// GetJob reads a job and returns it with its ETag.
func (c *Client) GetJob(ctx context.Context, id string) (Job, string, error) {
	if err := ValidateID("job_id", id); err != nil {
		return Job{}, "", err
	}
	var out Job
	etag, err := c.do(ctx, http.MethodGet, c.tenantPath("jobs", id), nil, nil, &out, true)
	return out, etag, err
}

// ClaimJob atomically claims a queued job for workerID.
func (c *Client) ClaimJob(ctx context.Context, id, workerID string) (Job, string, error) {
	if err := ValidateID("job_id", id); err != nil {
		return Job{}, "", err
	}
	if workerID == "" {
		return Job{}, "", errors.New("worker_id must not be empty")
	}
	body := struct {
		WorkerID string `json:"worker_id"`
	}{workerID}
	var out Job
	etag, err := c.do(ctx, http.MethodPost, c.tenantPath("jobs", id, "claim"), body, nil, &out, true)
	return out, etag, err
}

// tenantPath builds /v1/orgs/{org}/projects/{project}/<segments...> with every
// segment escaped.
func (c *Client) tenantPath(segments ...string) string {
	var b strings.Builder
	b.WriteString("/v1/orgs/")
	b.WriteString(url.PathEscape(c.org))
	b.WriteString("/projects/")
	b.WriteString(url.PathEscape(c.project))
	for _, s := range segments {
		b.WriteByte('/')
		b.WriteString(url.PathEscape(s))
	}
	return b.String()
}

func (c *Client) do(ctx context.Context, method, path string, body any, hdr http.Header, out any, auth bool) (string, error) {
	var rdr io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return "", fmt.Errorf("encoding request: %w", err)
		}
		rdr = bytes.NewReader(data)
	}

	// Split off a query string so the path part stays escaped as built.
	rawPath, rawQuery, _ := strings.Cut(path, "?")
	u := *c.base
	unescaped, err := url.PathUnescape(rawPath)
	if err != nil {
		return "", fmt.Errorf("invalid request path: %w", err)
	}
	u.Path = c.base.Path + unescaped
	u.RawPath = c.base.EscapedPath() + rawPath
	u.RawQuery = rawQuery

	req, err := http.NewRequestWithContext(ctx, method, u.String(), rdr)
	if err != nil {
		return "", fmt.Errorf("building request: %w", err)
	}
	for k, vs := range hdr {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if auth {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return "", c.sanitize(fmt.Errorf("request to sync failed: %w", err))
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return "", c.sanitize(fmt.Errorf("reading sync response: %w", err))
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", &APIError{Status: resp.StatusCode, Message: c.errorMessage(data)}
	}

	etag := strings.Trim(resp.Header.Get("ETag"), `"`)
	if out != nil && len(bytes.TrimSpace(data)) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return "", fmt.Errorf("decoding sync response: %w", err)
		}
	}
	return etag, nil
}

// errorMessage extracts a short message from an error body, which is either
// {"error": "..."} or plain text.
func (c *Client) errorMessage(data []byte) string {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return ""
	}
	var j struct {
		Error string `json:"error"`
	}
	msg := string(data)
	if json.Unmarshal(data, &j) == nil && j.Error != "" {
		msg = j.Error
	}
	if len(msg) > 500 {
		msg = msg[:500] + "..."
	}
	return strings.ReplaceAll(msg, c.apiKey, "[redacted]")
}

// sanitize makes sure an error can never reveal the API key.
func (c *Client) sanitize(err error) error {
	msg := err.Error()
	if strings.Contains(msg, c.apiKey) {
		return errors.New(strings.ReplaceAll(msg, c.apiKey, "[redacted]"))
	}
	return err
}

func quoteETag(etag string) string {
	etag = strings.Trim(etag, `"`)
	return `"` + etag + `"`
}
