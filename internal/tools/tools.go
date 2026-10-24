// Package tools registers the Sync MCP tools on an MCP server.
package tools

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/aixgo-dev/sync-mcp/internal/syncclient"
)

// Names of the registered tools.
const (
	ToolHealth        = "sync_health"
	ToolSessionRead   = "sync_session_read"
	ToolSessionWrite  = "sync_session_write"
	ToolMessageAppend = "sync_message_append"
	ToolMessageList   = "sync_message_list"
	ToolJobCreate     = "sync_job_create"
	ToolJobGet        = "sync_job_get"
	ToolJobClaim      = "sync_job_claim"
)

const maxListLimit = 1000

// Syncer is the subset of the Sync client the tools need.
type Syncer interface {
	Health(ctx context.Context) (map[string]any, error)
	GetSession(ctx context.Context, id string) (syncclient.Session, string, error)
	PutSession(ctx context.Context, id string, s syncclient.Session, ifMatch string) (syncclient.Session, string, error)
	AppendMessage(ctx context.Context, threadID string, m syncclient.NewMessage) (syncclient.Message, string, error)
	ListMessages(ctx context.Context, threadID, afterID string, limit int) ([]syncclient.Message, error)
	CreateJob(ctx context.Context, kind string, payload any) (syncclient.Job, string, error)
	GetJob(ctx context.Context, id string) (syncclient.Job, string, error)
	ClaimJob(ctx context.Context, id, workerID string) (syncclient.Job, string, error)
}

// Argument and result types. The jsonschema tags become the descriptions in
// each tool's input schema.

type healthArgs struct{}

type healthResult struct {
	Status string `json:"status" jsonschema:"health status reported by the server, normally ok"`
}

type sessionReadArgs struct {
	SessionID string `json:"session_id" jsonschema:"session identifier (letters, digits, '.', '_' and '-')"`
}

type sessionResult struct {
	Session syncclient.Session `json:"session" jsonschema:"the session document"`
	ETag    string             `json:"etag" jsonschema:"version of the session; pass it as if_match to sync_session_write to update it safely"`
}

type sessionWriteArgs struct {
	SessionID string            `json:"session_id" jsonschema:"session identifier (letters, digits, '.', '_' and '-')"`
	Status    string            `json:"status,omitempty" jsonschema:"session status: open or closed (default open)"`
	ThreadID  string            `json:"thread_id" jsonschema:"thread this session is attached to"`
	WorkerID  string            `json:"worker_id,omitempty" jsonschema:"identifier of the worker handling the session"`
	Labels    map[string]string `json:"labels,omitempty" jsonschema:"free-form string labels"`
	CreatedAt string            `json:"created_at,omitempty" jsonschema:"RFC 3339 creation time; when updating, pass the value from sync_session_read to preserve it"`
	IfMatch   string            `json:"if_match,omitempty" jsonschema:"ETag from a previous read or write. Omit to create a new session (fails with 409 if it already exists). Provide it to update (fails with 412 if the session changed since that version)"`
}

type messageAppendArgs struct {
	ThreadID      string         `json:"thread_id" jsonschema:"thread identifier (letters, digits, '.', '_' and '-')"`
	Role          string         `json:"role" jsonschema:"message role: user, assistant, system, tool or agent"`
	Content       string         `json:"content" jsonschema:"message text"`
	SenderActorID string         `json:"sender_actor_id,omitempty" jsonschema:"optional identifier of the sending actor"`
	Refs          map[string]any `json:"refs,omitempty" jsonschema:"optional structured references attached to the message"`
}

type messageAppendResult struct {
	Message syncclient.Message `json:"message" jsonschema:"the stored message, including its generated id and created_at"`
	ETag    string             `json:"etag" jsonschema:"version of the stored message"`
}

type messageListArgs struct {
	ThreadID string `json:"thread_id" jsonschema:"thread identifier"`
	AfterID  string `json:"after_id,omitempty" jsonschema:"return only messages after the message with this id (for paging)"`
	Limit    int    `json:"limit,omitempty" jsonschema:"maximum number of messages to return, 1 to 1000 (server default 100)"`
}

type messageListResult struct {
	Messages []syncclient.Message `json:"messages" jsonschema:"messages in ascending created_at order"`
	Count    int                  `json:"count" jsonschema:"number of messages returned"`
	LastID   string               `json:"last_id,omitempty" jsonschema:"id of the last message returned; pass it as after_id to fetch the next page"`
}

type jobCreateArgs struct {
	Kind    string `json:"kind" jsonschema:"job kind: code_offload, command, schedule or custom"`
	Payload any    `json:"payload,omitempty" jsonschema:"optional JSON payload describing the work"`
}

type jobGetArgs struct {
	JobID string `json:"job_id" jsonschema:"job identifier returned by sync_job_create"`
}

type jobClaimArgs struct {
	JobID    string `json:"job_id" jsonschema:"job identifier"`
	WorkerID string `json:"worker_id" jsonschema:"identifier of the worker claiming the job"`
}

type jobResult struct {
	Job  syncclient.Job `json:"job" jsonschema:"the job document"`
	ETag string         `json:"etag" jsonschema:"version of the job"`
}

// Register adds all Sync tools to server.
func Register(server *mcp.Server, c Syncer) {
	readOnly := &mcp.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: boolPtr(false)}
	additive := &mcp.ToolAnnotations{DestructiveHint: boolPtr(false), OpenWorldHint: boolPtr(false)}
	overwrite := &mcp.ToolAnnotations{DestructiveHint: boolPtr(true), IdempotentHint: true, OpenWorldHint: boolPtr(false)}

	mcp.AddTool(server, &mcp.Tool{
		Name:        ToolHealth,
		Description: "Check that the Sync server is reachable. Calls GET /healthz, which needs no credentials.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ healthArgs) (*mcp.CallToolResult, healthResult, error) {
		out, err := c.Health(ctx)
		if err != nil {
			return nil, healthResult{}, toolError(err)
		}
		status, _ := out["status"].(string)
		return nil, healthResult{Status: status}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        ToolSessionRead,
		Description: "Read a coordination session by id. Returns the session and its ETag, which is the version to pass as if_match when updating. Fails with a 404 error if the session does not exist.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in sessionReadArgs) (*mcp.CallToolResult, sessionResult, error) {
		s, etag, err := c.GetSession(ctx, in.SessionID)
		if err != nil {
			return nil, sessionResult{}, toolError(err)
		}
		return nil, sessionResult{Session: s, ETag: etag}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name: ToolSessionWrite,
		Description: "Create or update a coordination session with compare-and-swap. " +
			"Omit if_match to create the session (409 if it already exists). " +
			"Pass the ETag from sync_session_read or a previous write as if_match to update it (412 if someone else changed it first; re-read and retry). " +
			"An update replaces the whole document, so include every field you want to keep. Returns the stored session and its new ETag.",
		Annotations: overwrite,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in sessionWriteArgs) (*mcp.CallToolResult, sessionResult, error) {
		status := in.Status
		if status == "" {
			status = "open"
		}
		if status != "open" && status != "closed" {
			return nil, sessionResult{}, toolError(fmt.Errorf("status must be \"open\" or \"closed\", got %q", status))
		}
		if in.ThreadID != "" {
			if err := syncclient.ValidateID("thread_id", in.ThreadID); err != nil {
				return nil, sessionResult{}, toolError(err)
			}
		}
		if strings.TrimSpace(in.ThreadID) == "" {
			return nil, sessionResult{}, toolError(errors.New("thread_id must not be empty"))
		}
		s, etag, err := c.PutSession(ctx, in.SessionID, syncclient.Session{
			Status:    status,
			ThreadID:  in.ThreadID,
			WorkerID:  in.WorkerID,
			Labels:    in.Labels,
			CreatedAt: in.CreatedAt,
		}, in.IfMatch)
		if err != nil {
			return nil, sessionResult{}, toolError(err)
		}
		return nil, sessionResult{Session: s, ETag: etag}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        ToolMessageAppend,
		Description: "Append a message to a thread. Messages are immutable once written. Returns the stored message with its generated id and timestamp.",
		Annotations: additive,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in messageAppendArgs) (*mcp.CallToolResult, messageAppendResult, error) {
		switch in.Role {
		case "user", "assistant", "system", "tool", "agent":
		default:
			return nil, messageAppendResult{}, toolError(fmt.Errorf("role must be one of user, assistant, system, tool, agent; got %q", in.Role))
		}
		m, etag, err := c.AppendMessage(ctx, in.ThreadID, syncclient.NewMessage{
			SenderActorID: in.SenderActorID,
			Role:          in.Role,
			Content:       in.Content,
			Refs:          in.Refs,
		})
		if err != nil {
			return nil, messageAppendResult{}, toolError(err)
		}
		return nil, messageAppendResult{Message: m, ETag: etag}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        ToolMessageList,
		Description: "List the messages in a thread in ascending created_at order. Use after_id with the returned last_id to page forward.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in messageListArgs) (*mcp.CallToolResult, messageListResult, error) {
		if in.Limit < 0 || in.Limit > maxListLimit {
			return nil, messageListResult{}, toolError(fmt.Errorf("limit must be between 1 and %d", maxListLimit))
		}
		msgs, err := c.ListMessages(ctx, in.ThreadID, in.AfterID, in.Limit)
		if err != nil {
			return nil, messageListResult{}, toolError(err)
		}
		res := messageListResult{Messages: msgs, Count: len(msgs)}
		if len(msgs) > 0 {
			res.LastID = msgs[len(msgs)-1].ID
		}
		return nil, res, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        ToolJobCreate,
		Description: "Create a queued job. Returns the job with its generated id and ETag. Workers pick it up with sync_job_claim.",
		Annotations: additive,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in jobCreateArgs) (*mcp.CallToolResult, jobResult, error) {
		switch in.Kind {
		case "code_offload", "command", "schedule", "custom":
		default:
			return nil, jobResult{}, toolError(fmt.Errorf("kind must be one of code_offload, command, schedule, custom; got %q", in.Kind))
		}
		j, etag, err := c.CreateJob(ctx, in.Kind, in.Payload)
		if err != nil {
			return nil, jobResult{}, toolError(err)
		}
		return nil, jobResult{Job: j, ETag: etag}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        ToolJobGet,
		Description: "Read a job by id, including its status (queued or claimed), assignee and result. Fails with a 404 error if the job does not exist.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in jobGetArgs) (*mcp.CallToolResult, jobResult, error) {
		j, etag, err := c.GetJob(ctx, in.JobID)
		if err != nil {
			return nil, jobResult{}, toolError(err)
		}
		return nil, jobResult{Job: j, ETag: etag}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        ToolJobClaim,
		Description: "Atomically claim a queued job for a worker. Exactly one claimer wins: a 409 error means the job was already claimed, and a 412 error means another claim raced this one; neither should be retried blindly.",
		Annotations: &mcp.ToolAnnotations{DestructiveHint: boolPtr(false), OpenWorldHint: boolPtr(false)},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in jobClaimArgs) (*mcp.CallToolResult, jobResult, error) {
		j, etag, err := c.ClaimJob(ctx, in.JobID, in.WorkerID)
		if err != nil {
			return nil, jobResult{}, toolError(err)
		}
		return nil, jobResult{Job: j, ETag: etag}, nil
	})
}

// toolError turns an error into text that is useful to a model. Returned from
// a tool handler, it becomes an isError result rather than a protocol error.
func toolError(err error) error {
	var apiErr *syncclient.APIError
	if !errors.As(err, &apiErr) {
		return err
	}
	var hint string
	switch apiErr.Status {
	case http.StatusBadRequest:
		hint = "The request was rejected as invalid; check the arguments."
	case http.StatusUnauthorized:
		hint = "The configured API key was missing, invalid or revoked. This cannot be fixed by retrying; ask the operator to check the server's credentials."
	case http.StatusForbidden:
		hint = "The configured API key is not allowed to use this route or tenant. Retrying will not help."
	case http.StatusNotFound:
		hint = "The resource does not exist."
	case http.StatusConflict:
		hint = "The resource already exists or is in a state that does not allow this operation."
	case http.StatusPreconditionFailed:
		hint = "The resource changed since the version you passed. Re-read it to get the current ETag and retry if still appropriate."
	}
	if hint == "" {
		return apiErr
	}
	return fmt.Errorf("%w. %s", apiErr, hint)
}

func boolPtr(b bool) *bool { return &b }
