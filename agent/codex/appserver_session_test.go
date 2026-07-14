package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

func TestAppServerSession_ApplyThreadRuntimeState(t *testing.T) {
	s := &appServerSession{}
	effort := "xhigh"

	s.applyThreadRuntimeState("/tmp/project", "gpt-5.4", &effort)

	if got := s.GetWorkDir(); got != "/tmp/project" {
		t.Fatalf("GetWorkDir() = %q, want /tmp/project", got)
	}
	if got := s.GetModel(); got != "gpt-5.4" {
		t.Fatalf("GetModel() = %q, want gpt-5.4", got)
	}
	if got := s.GetReasoningEffort(); got != "xhigh" {
		t.Fatalf("GetReasoningEffort() = %q, want xhigh", got)
	}
}

func TestAppServerSession_HandleRateLimitsUpdatedCachesUsage(t *testing.T) {
	s := &appServerSession{}
	raw, err := json.Marshal(appServerRateLimitsResponse{
		RateLimits: appServerRateLimitSnapshot{
			LimitID:   "codex",
			PlanType:  "pro",
			Primary:   &appServerRateLimitWindow{UsedPercent: 25, WindowDurationMins: 15, ResetsAt: 1730947200},
			Secondary: &appServerRateLimitWindow{UsedPercent: 42, WindowDurationMins: 60, ResetsAt: 1730950800},
			Credits:   &appServerCreditsSnapshot{HasCredits: true, Unlimited: false},
		},
	})
	if err != nil {
		t.Fatalf("marshal notification: %v", err)
	}

	s.handleNotification("account/rateLimits/updated", raw)

	report, err := s.GetUsage(context.Background())
	if err != nil {
		t.Fatalf("GetUsage() returned error: %v", err)
	}
	if report.Provider != "codex" {
		t.Fatalf("provider = %q, want codex", report.Provider)
	}
	if report.Plan != "pro" {
		t.Fatalf("plan = %q, want pro", report.Plan)
	}
	if len(report.Buckets) != 1 {
		t.Fatalf("buckets = %d, want 1", len(report.Buckets))
	}
	if got := report.Buckets[0].Name; got != "codex" {
		t.Fatalf("bucket name = %q, want codex", got)
	}
	if got := report.Buckets[0].Windows[0].WindowSeconds; got != 15*60 {
		t.Fatalf("primary window seconds = %d, want %d", got, 15*60)
	}
	if got := report.Buckets[0].Windows[1].UsedPercent; got != 42 {
		t.Fatalf("secondary used percent = %d, want 42", got)
	}
	if report.Credits == nil || !report.Credits.HasCredits {
		t.Fatalf("credits = %#v, want has credits", report.Credits)
	}
}

func TestAppServerSession_HandleThreadTokenUsageUpdatedCachesContextUsage(t *testing.T) {
	s := &appServerSession{}
	s.threadID.Store("thread-1")
	raw, err := json.Marshal(appServerThreadTokenUsageNotification{
		ThreadID: "thread-1",
		TurnID:   "turn-1",
		TokenUsage: struct {
			Total              codexTokenUsage `json:"total"`
			Last               codexTokenUsage `json:"last"`
			ModelContextWindow int             `json:"modelContextWindow"`
		}{
			Total: codexTokenUsage{
				TotalTokens:           52011395,
				InputTokens:           51847383,
				CachedInputTokens:     48187904,
				OutputTokens:          164012,
				ReasoningOutputTokens: 78910,
			},
			Last: codexTokenUsage{
				TotalTokens:           41061,
				InputTokens:           40849,
				CachedInputTokens:     36864,
				OutputTokens:          212,
				ReasoningOutputTokens: 32,
			},
			ModelContextWindow: 258400,
		},
	})
	if err != nil {
		t.Fatalf("marshal notification: %v", err)
	}

	s.handleNotification("thread/tokenUsage/updated", raw)

	usage := s.GetContextUsage()
	if usage == nil {
		t.Fatal("GetContextUsage() = nil, want cached context usage")
	}
	if usage.UsedTokens != 41061 {
		t.Fatalf("used tokens = %d, want 41061", usage.UsedTokens)
	}
	if usage.BaselineTokens != codexContextBaselineTokens {
		t.Fatalf("baseline tokens = %d, want %d", usage.BaselineTokens, codexContextBaselineTokens)
	}
	if usage.TotalTokens != 41061 {
		t.Fatalf("total tokens = %d, want 41061", usage.TotalTokens)
	}
	if usage.ContextWindow != 258400 {
		t.Fatalf("context window = %d, want 258400", usage.ContextWindow)
	}
	if usage.CachedInputTokens != 36864 {
		t.Fatalf("cached input tokens = %d, want 36864", usage.CachedInputTokens)
	}
	if usage.InputTokens != 40849 {
		t.Fatalf("input tokens = %d, want 40849", usage.InputTokens)
	}
}

func TestAppServerSession_RequestTimeoutIncludesBlockedStdinWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stdin := newBlockingWriteCloser()
	defer func() { _ = stdin.Close() }()

	s := &appServerSession{
		ctx:     ctx,
		cancel:  cancel,
		events:  make(chan core.Event),
		stdin:   stdin,
		pending: make(map[int64]chan rpcResponseEnvelope),
	}

	done := make(chan error, 1)
	go func() {
		var out map[string]any
		done <- s.requestWithTimeout("turn/start", map[string]any{
			"input": strings.Repeat("x", 1024),
		}, &out, 25*time.Millisecond)
	}()

	select {
	case <-stdin.started:
	case <-time.After(time.Second):
		t.Fatal("request did not attempt to write to stdin")
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("requestWithTimeout returned nil, want write timeout")
		}
		if !strings.Contains(err.Error(), "turn/start") || !strings.Contains(err.Error(), "write timed out") {
			t.Fatalf("error = %q, want turn/start write timeout", err.Error())
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("requestWithTimeout did not return while stdin write was blocked")
	}

	if !stdin.Closed() {
		t.Fatal("blocked stdin was not closed after timeout")
	}
}

func TestMapAppServerRateLimits_PrefersMultiBucketView(t *testing.T) {
	report := mapAppServerRateLimits(appServerRateLimitsResponse{
		RateLimits: appServerRateLimitSnapshot{
			LimitID:  "legacy",
			PlanType: "team",
			Primary:  &appServerRateLimitWindow{UsedPercent: 99, WindowDurationMins: 15},
		},
		RateLimitsByLimitID: map[string]appServerRateLimitSnapshot{
			"codex": {
				LimitID:   "codex",
				LimitName: "Codex",
				PlanType:  "team",
				Primary:   &appServerRateLimitWindow{UsedPercent: 10, WindowDurationMins: 15},
			},
			"codex_other": {
				LimitID:  "codex_other",
				PlanType: "team",
				Primary:  &appServerRateLimitWindow{UsedPercent: 20, WindowDurationMins: 60},
			},
		},
	})

	if report.Plan != "team" {
		t.Fatalf("plan = %q, want team", report.Plan)
	}
	if len(report.Buckets) != 2 {
		t.Fatalf("buckets = %d, want 2", len(report.Buckets))
	}
	if report.Buckets[0].Name != "Codex" {
		t.Fatalf("first bucket = %q, want Codex", report.Buckets[0].Name)
	}
	if report.Buckets[1].Name != "codex_other" {
		t.Fatalf("second bucket = %q, want codex_other", report.Buckets[1].Name)
	}
}

func TestAppServerSession_HandleRequestUserInputEmitsAskQuestion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stdin := &lockedWriteCloser{}
	s := &appServerSession{
		events:           make(chan core.Event, 4),
		ctx:              ctx,
		pendingApprovals: make(map[string]chan core.PermissionResult),
		stdin:            stdin,
	}

	s.handleServerRequest(serverRequestProbe(t, `"rui-1"`, "item/tool/requestUserInput", map[string]any{
		"threadId": "thread-1",
		"turnId":   "turn-1",
		"itemId":   "call-1",
		"questions": []any{
			map[string]any{
				"id":       "database",
				"header":   "Database",
				"question": "Which database should we use?",
				"isOther":  true,
				"isSecret": false,
				"options": []any{
					map[string]any{"label": "Postgres", "description": "Use the existing relational database"},
					map[string]any{"label": "SQLite", "description": "Keep it embedded"},
				},
			},
		},
	}))

	var event core.Event
	select {
	case event = <-s.events:
	case <-time.After(time.Second):
		t.Fatal("expected AskUserQuestion event")
	}
	if event.Type != core.EventPermissionRequest {
		t.Fatalf("event type = %s, want %s", event.Type, core.EventPermissionRequest)
	}
	if event.ToolName != "AskUserQuestion" {
		t.Fatalf("tool name = %q, want AskUserQuestion", event.ToolName)
	}
	if event.RequestID != `"rui-1"` {
		t.Fatalf("request id = %q, want raw JSON id", event.RequestID)
	}
	if len(event.Questions) != 1 {
		t.Fatalf("questions = %d, want 1", len(event.Questions))
	}
	q := event.Questions[0]
	if q.Question != "Which database should we use?" || q.Header != "Database" {
		t.Fatalf("question = %#v", q)
	}
	if len(q.Options) != 2 || q.Options[0].Label != "Postgres" || q.Options[1].Description != "Keep it embedded" {
		t.Fatalf("options = %#v", q.Options)
	}
	if stdin.String() != "" {
		t.Fatalf("request_user_input should not write before the answer, got %q", stdin.String())
	}
}

func TestAppServerSession_HandleRequestUserInputWritesCodexResponse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stdin := &lockedWriteCloser{}
	s := &appServerSession{
		events:           make(chan core.Event, 4),
		ctx:              ctx,
		pendingApprovals: make(map[string]chan core.PermissionResult),
		stdin:            stdin,
	}

	s.handleServerRequest(serverRequestProbe(t, `"rui-2"`, "item/tool/requestUserInput", map[string]any{
		"threadId": "thread-1",
		"turnId":   "turn-1",
		"itemId":   "call-2",
		"questions": []any{
			map[string]any{
				"id":       "database",
				"header":   "Database",
				"question": "Which database should we use?",
				"options": []any{
					map[string]any{"label": "Postgres", "description": "Use the existing relational database"},
					map[string]any{"label": "SQLite", "description": "Keep it embedded"},
				},
			},
		},
	}))

	var event core.Event
	select {
	case event = <-s.events:
	case <-time.After(time.Second):
		t.Fatal("expected AskUserQuestion event")
	}
	if err := s.RespondPermission(event.RequestID, core.PermissionResult{
		Behavior: "allow",
		UpdatedInput: map[string]any{
			"answers": map[string]any{
				"Which database should we use?": "Postgres",
			},
		},
	}); err != nil {
		t.Fatalf("RespondPermission() error = %v", err)
	}

	line := waitForWrittenJSONLine(t, stdin)
	var envelope struct {
		JSONRPC string `json:"jsonrpc"`
		ID      string `json:"id"`
		Result  struct {
			Answers map[string]struct {
				Answers []string `json:"answers"`
			} `json:"answers"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(line), &envelope); err != nil {
		t.Fatalf("decode response %q: %v", line, err)
	}
	if envelope.JSONRPC != "2.0" || envelope.ID != "rui-2" {
		t.Fatalf("envelope = %#v", envelope)
	}
	got := envelope.Result.Answers["database"].Answers
	if len(got) != 1 || got[0] != "Postgres" {
		t.Fatalf("answers[database] = %#v, want [Postgres]", got)
	}
}

func TestAppServerSession_UnknownItemDoesNotDrainFinalText(t *testing.T) {
	s := newAppServerEventTestSession()

	notifyAppServerTest(t, s, "item/completed", map[string]any{
		"item": map[string]any{"type": "agentMessage", "text": "I found the relevant path."},
	})
	notifyAppServerTest(t, s, "item/started", map[string]any{
		"item": map[string]any{"type": "futureToolCall"},
	})
	notifyAppServerTest(t, s, "item/completed", map[string]any{
		"item": map[string]any{"type": "futureToolCall"},
	})
	notifyAppServerTest(t, s, "turn/completed", map[string]any{})

	events := drainAppServerTestEvents(s)
	if len(events) != 2 {
		t.Fatalf("events = %#v, want EventText followed by EventResult", events)
	}
	if events[0].Type != core.EventText || events[0].Content != "I found the relevant path." {
		t.Fatalf("events[0] = %#v, want preserved EventText", events[0])
	}
	if events[1].Type != core.EventResult || !events[1].Done {
		t.Fatalf("events[1] = %#v, want completed EventResult", events[1])
	}
}

func TestAppServerSession_FunctionCallKeepsAssistantTextAsFinalFallback(t *testing.T) {
	s := newAppServerEventTestSession()

	notifyAppServerTest(t, s, "item/completed", map[string]any{
		"item": map[string]any{"type": "agentMessage", "text": "I delegated the review."},
	})
	functionCall := map[string]any{
		"type":      "function_call",
		"name":      "spawn_agent",
		"call_id":   "call-1",
		"arguments": `{"agent_type":"critic"}`,
	}
	notifyAppServerTest(t, s, "item/started", map[string]any{"item": functionCall})
	notifyAppServerTest(t, s, "item/completed", map[string]any{"item": functionCall})
	notifyAppServerTest(t, s, "item/completed", map[string]any{
		"item": map[string]any{
			"type":    "function_call_output",
			"call_id": "call-1",
			"output":  "agent Cicero spawned",
		},
	})
	notifyAppServerTest(t, s, "turn/completed", map[string]any{})

	events := drainAppServerTestEvents(s)
	assertAppServerEvent(t, events, core.EventToolUse, "spawn_agent", `{"agent_type":"critic"}`)
	assertAppServerEvent(t, events, core.EventToolResult, "spawn_agent", "agent Cicero spawned")
	textIndex := appServerEventIndex(events, core.EventText)
	resultIndex := appServerEventIndex(events, core.EventResult)
	if textIndex < 0 || resultIndex < 0 || textIndex >= resultIndex {
		t.Fatalf("events = %#v, want EventText before EventResult", events)
	}
	if events[textIndex].Content != "I delegated the review." {
		t.Fatalf("final fallback = %q, want delegated review text", events[textIndex].Content)
	}
}

func TestAppServerSession_CollabAgentCallKeepsAssistantTextAsFinalFallback(t *testing.T) {
	s := newAppServerEventTestSession()

	notifyAppServerTest(t, s, "item/completed", map[string]any{
		"item": map[string]any{"type": "agentMessage", "text": "Waiting for the reviewer."},
	})
	notifyAppServerTest(t, s, "item/started", map[string]any{
		"item": map[string]any{
			"type":   "collabAgentToolCall",
			"tool":   "wait",
			"prompt": "Cicero",
		},
	})
	notifyAppServerTest(t, s, "item/completed", map[string]any{
		"item": map[string]any{
			"type":         "collabAgentToolCall",
			"tool":         "wait",
			"status":       "completed",
			"agentsStates": map[string]any{"Cicero": "completed"},
		},
	})
	notifyAppServerTest(t, s, "turn/completed", map[string]any{})

	events := drainAppServerTestEvents(s)
	assertAppServerEvent(t, events, core.EventToolUse, "wait", "Cicero")
	assertAppServerEvent(t, events, core.EventToolResult, "wait", `{"Cicero":"completed"}`)
	textIndex := appServerEventIndex(events, core.EventText)
	resultIndex := appServerEventIndex(events, core.EventResult)
	if textIndex < 0 || resultIndex < 0 || textIndex >= resultIndex {
		t.Fatalf("events = %#v, want EventText before EventResult", events)
	}
}

func TestAppServerSession_FinalMessageTakesPrecedenceOverFallback(t *testing.T) {
	s := newAppServerEventTestSession()

	notifyAppServerTest(t, s, "item/completed", map[string]any{
		"item": map[string]any{"type": "agentMessage", "text": "I am checking the repository."},
	})
	notifyAppServerTest(t, s, "item/started", map[string]any{
		"item": map[string]any{"type": "commandExecution", "command": "git status"},
	})
	notifyAppServerTest(t, s, "item/completed", map[string]any{
		"item": map[string]any{"type": "agentMessage", "text": "The repository is clean."},
	})
	notifyAppServerTest(t, s, "turn/completed", map[string]any{})

	events := drainAppServerTestEvents(s)
	var textEvents []core.Event
	for _, event := range events {
		if event.Type == core.EventText {
			textEvents = append(textEvents, event)
		}
	}
	if len(textEvents) != 1 || textEvents[0].Content != "The repository is clean." {
		t.Fatalf("text events = %#v, want only the final agent message", textEvents)
	}
}

func TestAppServerSession_ChildTurnDoesNotReplaceOrCompleteParentTurn(t *testing.T) {
	s := newAppServerEventTestSession()
	s.threadID.Store("parent-thread")
	s.currentTurn = "parent-turn"
	s.pendingMsgs = []string{"parent response"}

	notifyAppServerTest(t, s, "turn/started", map[string]any{
		"threadId": "child-thread",
		"turn":     map[string]any{"id": "child-turn"},
	})
	notifyAppServerTest(t, s, "turn/completed", map[string]any{
		"threadId": "child-thread",
		"turn":     map[string]any{"id": "child-turn"},
	})
	notifyAppServerTest(t, s, "thread/status/changed", map[string]any{
		"threadId": "child-thread",
		"status":   map[string]any{"type": "idle"},
	})
	notifyAppServerTest(t, s, "item/completed", map[string]any{
		"threadId": "child-thread",
		"turnId":   "child-turn",
		"item":     map[string]any{"type": "agentMessage", "text": "child response"},
	})
	notifyAppServerTest(t, s, "item/started", map[string]any{
		"threadId": "child-thread",
		"turnId":   "child-turn",
		"item":     map[string]any{"type": "commandExecution", "command": "go test ./..."},
	})

	s.stateMu.Lock()
	currentTurn := s.currentTurn
	pendingMsgs := append([]string(nil), s.pendingMsgs...)
	s.stateMu.Unlock()
	if currentTurn != "parent-turn" {
		t.Fatalf("currentTurn = %q, want parent-turn", currentTurn)
	}
	if len(pendingMsgs) != 1 || pendingMsgs[0] != "parent response" {
		t.Fatalf("pendingMsgs = %#v, want parent response preserved", pendingMsgs)
	}
	if events := drainAppServerTestEvents(s); len(events) != 0 {
		t.Fatalf("child turn emitted events: %#v", events)
	}

	notifyAppServerTest(t, s, "turn/completed", map[string]any{
		"threadId": "parent-thread",
		"turn":     map[string]any{"id": "parent-turn"},
	})
	events := drainAppServerTestEvents(s)
	if len(events) != 2 || events[0].Type != core.EventText || events[1].Type != core.EventResult {
		t.Fatalf("parent completion events = %#v, want EventText followed by EventResult", events)
	}
}

func TestAppServerSession_StaleParentTurnCompletionDoesNotCompleteActiveTurn(t *testing.T) {
	s := newAppServerEventTestSession()
	s.threadID.Store("parent-thread")
	s.currentTurn = "active-turn"

	notifyAppServerTest(t, s, "turn/completed", map[string]any{
		"threadId": "parent-thread",
		"turn":     map[string]any{"id": "stale-turn"},
	})

	s.stateMu.Lock()
	currentTurn := s.currentTurn
	s.stateMu.Unlock()
	if currentTurn != "active-turn" {
		t.Fatalf("currentTurn = %q, want active-turn", currentTurn)
	}
	if events := drainAppServerTestEvents(s); len(events) != 0 {
		t.Fatalf("stale turn completion emitted events: %#v", events)
	}
}

func TestAppServerSession_FailedTurnCompletionEmitsError(t *testing.T) {
	s := newAppServerEventTestSession()

	notifyAppServerTest(t, s, "turn/completed", map[string]any{
		"threadId": "thread-1",
		"turn": map[string]any{
			"id":     "turn-1",
			"status": "failed",
			"error":  map[string]any{"message": "agent loop died unexpectedly"},
		},
	})

	events := drainAppServerTestEvents(s)
	if len(events) != 1 || events[0].Type != core.EventError {
		t.Fatalf("events = %#v, want one EventError", events)
	}
	if events[0].Error == nil || events[0].Error.Error() != "agent loop died unexpectedly" {
		t.Fatalf("error = %v, want agent loop failure", events[0].Error)
	}
}

func TestAppServerSession_InterruptedTurnCompletionEmitsError(t *testing.T) {
	s := newAppServerEventTestSession()

	notifyAppServerTest(t, s, "turn/completed", map[string]any{
		"threadId": "thread-1",
		"turn": map[string]any{
			"id":     "turn-1",
			"status": "interrupted",
		},
	})

	events := drainAppServerTestEvents(s)
	if len(events) != 1 || events[0].Type != core.EventError {
		t.Fatalf("events = %#v, want one EventError", events)
	}
	if events[0].Error == nil || events[0].Error.Error() != "codex turn interrupted" {
		t.Fatalf("error = %v, want interrupted turn error", events[0].Error)
	}
}

func TestAppServerSession_MissingThreadIDDoesNotMutateParentTurn(t *testing.T) {
	s := newAppServerEventTestSession()
	s.currentTurn = "parent-turn"
	s.pendingMsgs = []string{"parent response"}

	raw, err := json.Marshal(map[string]any{
		"turn": map[string]any{"id": "unknown-turn"},
	})
	if err != nil {
		t.Fatalf("marshal notification: %v", err)
	}
	s.handleNotification("turn/started", raw)

	s.stateMu.Lock()
	currentTurn := s.currentTurn
	pendingMsgs := append([]string(nil), s.pendingMsgs...)
	s.stateMu.Unlock()
	if currentTurn != "parent-turn" {
		t.Fatalf("currentTurn = %q, want parent-turn", currentTurn)
	}
	if len(pendingMsgs) != 1 || pendingMsgs[0] != "parent response" {
		t.Fatalf("pendingMsgs = %#v, want parent response preserved", pendingMsgs)
	}
}

func newAppServerEventTestSession() *appServerSession {
	s := &appServerSession{
		events:      make(chan core.Event, 16),
		currentTurn: "turn-1",
	}
	s.threadID.Store("thread-1")
	return s
}

func notifyAppServerTest(t *testing.T, s *appServerSession, method string, params any) {
	t.Helper()
	if values, ok := params.(map[string]any); ok {
		if _, exists := values["threadId"]; !exists {
			values["threadId"] = s.CurrentSessionID()
		}
	}
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal %s notification: %v", method, err)
	}
	s.handleNotification(method, raw)
}

func drainAppServerTestEvents(s *appServerSession) []core.Event {
	var events []core.Event
	for len(s.events) > 0 {
		events = append(events, <-s.events)
	}
	return events
}

func assertAppServerEvent(t *testing.T, events []core.Event, eventType core.EventType, toolName, content string) {
	t.Helper()
	for _, event := range events {
		if event.Type != eventType || event.ToolName != toolName {
			continue
		}
		if eventType == core.EventToolUse && event.ToolInput == content {
			return
		}
		if eventType == core.EventToolResult && event.ToolResult == content {
			return
		}
	}
	t.Fatalf("events = %#v, missing %s for %s with content %q", events, eventType, toolName, content)
}

func appServerEventIndex(events []core.Event, eventType core.EventType) int {
	for i, event := range events {
		if event.Type == eventType {
			return i
		}
	}
	return -1
}

func TestAppServerSessionSteer_RequiresActiveTurn(t *testing.T) {
	s := &appServerSession{
		ctx:     context.Background(),
		pending: make(map[int64]chan rpcResponseEnvelope),
	}
	s.alive.Store(true)
	s.threadID.Store("thread-1")

	err := s.Steer("focus on failing tests first")
	if err == nil || err.Error() != "codex app-server has no active turn to steer" {
		t.Fatalf("Steer() error = %v, want no active turn error", err)
	}
}

func TestAppServerSessionSteer_RequestShape(t *testing.T) {
	stdin := &lockedWriteCloser{}
	s := &appServerSession{
		ctx:     context.Background(),
		stdin:   stdin,
		pending: make(map[int64]chan rpcResponseEnvelope),
	}
	s.alive.Store(true)
	s.threadID.Store("thread-1")
	s.currentTurn = "turn-1"

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			s.pendingMu.Lock()
			ch := s.pending[1]
			s.pendingMu.Unlock()
			if ch != nil {
				ch <- rpcResponseEnvelope{ID: int64(1), Result: json.RawMessage(`{"turnId":"turn-1"}`)}
				return
			}
		}
	}()

	if err := s.Steer("focus on failing tests first"); err != nil {
		t.Fatalf("Steer() error = %v", err)
	}
	wg.Wait()

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdin.String())), &payload); err != nil {
		t.Fatalf("decode steer payload: %v", err)
	}

	if got := payload["method"]; got != "turn/steer" {
		t.Fatalf("method = %#v, want turn/steer", got)
	}

	params, ok := payload["params"].(map[string]any)
	if !ok {
		t.Fatalf("params = %#v, want object", payload["params"])
	}
	if got := params["threadId"]; got != "thread-1" {
		t.Fatalf("threadId = %#v, want thread-1", got)
	}
	if got := params["expectedTurnId"]; got != "turn-1" {
		t.Fatalf("expectedTurnId = %#v, want turn-1", got)
	}

	input, ok := params["input"].([]any)
	if !ok || len(input) != 1 {
		t.Fatalf("input = %#v, want single-element array", params["input"])
	}
	item, ok := input[0].(map[string]any)
	if !ok {
		t.Fatalf("input[0] = %#v, want object", input[0])
	}
	if got := item["type"]; got != "text" {
		t.Fatalf("input[0].type = %#v, want text", got)
	}
	if got := item["text"]; got != "focus on failing tests first" {
		t.Fatalf("input[0].text = %#v, want steer text", got)
	}
}

func TestAppServerSessionSend_RecoversDeadAgentLoopOnFreshThread(t *testing.T) {
	var s *appServerSession
	var mu sync.Mutex
	var requests []map[string]any
	var turnStarts int
	stdin := &callbackWriteCloser{onWrite: func(p []byte) {
		var request map[string]any
		if err := json.Unmarshal(bytes.TrimSpace(p), &request); err != nil {
			panic(fmt.Sprintf("decode request: %v", err))
		}
		mu.Lock()
		requests = append(requests, request)
		method, _ := request["method"].(string)
		if method == "turn/start" {
			turnStarts++
		}
		currentTurnStart := turnStarts
		mu.Unlock()

		id := int64(request["id"].(float64))
		switch {
		case method == "turn/start" && currentTurnStart == 1:
			s.handleResponse(rpcResponseEnvelope{ID: id, Error: &rpcError{Message: "failed to start turn: internal error; agent loop died unexpectedly"}})
		case method == "thread/start":
			s.handleResponse(rpcResponseEnvelope{ID: id, Result: json.RawMessage(`{"thread":{"id":"thread-new"},"cwd":"/tmp/project","model":"gpt-5.6-sol","reasoningEffort":"high"}`)})
		case method == "turn/start" && currentTurnStart == 2:
			s.handleResponse(rpcResponseEnvelope{ID: id, Result: json.RawMessage(`{"turn":{"id":"turn-new"}}`)})
		default:
			s.handleResponse(rpcResponseEnvelope{ID: id, Error: &rpcError{Message: "unexpected request"}})
		}
	}}
	s = newScriptedAppServerSession(t, stdin)
	s.threadID.Store("thread-old")
	s.promptPreamble = "Project system prompt:\nStay precise."
	s.preambleSent = true

	if err := s.Send("continue the review", nil, nil); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if got := s.CurrentSessionID(); got != "thread-new" {
		t.Fatalf("CurrentSessionID() = %q, want thread-new", got)
	}
	if s.currentTurn != "turn-new" {
		t.Fatalf("currentTurn = %q, want turn-new", s.currentTurn)
	}
	if !s.Alive() {
		t.Fatal("session marked dead after successful recovery")
	}
	events := drainAppServerTestEvents(s)
	if len(events) != 1 || events[0].Type != core.EventSessionRecovered || events[0].SessionID != "thread-new" {
		t.Fatalf("events = %#v, want fresh-session recovery event", events)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 3 {
		t.Fatalf("requests = %d, want 3", len(requests))
	}
	wantMethods := []string{"turn/start", "thread/start", "turn/start"}
	for i, want := range wantMethods {
		if got := requests[i]["method"]; got != want {
			t.Fatalf("requests[%d].method = %#v, want %q", i, got, want)
		}
	}
	firstParams := requests[0]["params"].(map[string]any)
	secondParams := requests[2]["params"].(map[string]any)
	if firstParams["threadId"] != "thread-old" || secondParams["threadId"] != "thread-new" {
		t.Fatalf("turn thread IDs = %#v then %#v", firstParams["threadId"], secondParams["threadId"])
	}
	firstText := firstParams["input"].([]any)[0].(map[string]any)["text"].(string)
	secondText := secondParams["input"].([]any)[0].(map[string]any)["text"].(string)
	if firstText != "continue the review" {
		t.Fatalf("first prompt = %q, want user message only for resumed thread", firstText)
	}
	if !strings.Contains(secondText, "Stay precise.") || !strings.Contains(secondText, "continue the review") {
		t.Fatalf("recovery prompt = %q, want preamble and original user message", secondText)
	}
}

func TestAppServerSessionSend_FailedDeadAgentLoopRecoveryMarksSessionUnhealthy(t *testing.T) {
	var s *appServerSession
	stdin := &callbackWriteCloser{onWrite: func(p []byte) {
		var request map[string]any
		if err := json.Unmarshal(bytes.TrimSpace(p), &request); err != nil {
			panic(fmt.Sprintf("decode request: %v", err))
		}
		id := int64(request["id"].(float64))
		method, _ := request["method"].(string)
		message := "failed to start turn: internal error; agent loop died unexpectedly"
		if method == "thread/start" {
			message = "failed to start replacement thread"
		}
		s.handleResponse(rpcResponseEnvelope{ID: id, Error: &rpcError{Message: message}})
	}}
	s = newScriptedAppServerSession(t, stdin)
	s.threadID.Store("thread-old")

	err := s.Send("continue the review", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "fresh thread recovery") {
		t.Fatalf("Send() error = %v, want fresh thread recovery failure", err)
	}
	if s.Alive() {
		t.Fatal("session remained alive after failed recovery")
	}
}

func TestAppServerSessionSend_RetriesDeadAgentLoopOnlyOnce(t *testing.T) {
	var s *appServerSession
	var mu sync.Mutex
	var methods []string
	stdin := &callbackWriteCloser{onWrite: func(p []byte) {
		var request map[string]any
		if err := json.Unmarshal(bytes.TrimSpace(p), &request); err != nil {
			panic(fmt.Sprintf("decode request: %v", err))
		}
		id := int64(request["id"].(float64))
		method, _ := request["method"].(string)
		mu.Lock()
		methods = append(methods, method)
		mu.Unlock()
		if method == "thread/start" {
			s.handleResponse(rpcResponseEnvelope{ID: id, Result: json.RawMessage(`{"thread":{"id":"thread-new"}}`)})
			return
		}
		s.handleResponse(rpcResponseEnvelope{ID: id, Error: &rpcError{Message: "failed to start turn: internal error; agent loop died unexpectedly"}})
	}}
	s = newScriptedAppServerSession(t, stdin)
	s.threadID.Store("thread-old")

	err := s.Send("continue the review", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "fresh thread recovery retry") {
		t.Fatalf("Send() error = %v, want retry failure", err)
	}
	if s.Alive() {
		t.Fatal("session remained alive after retry failure")
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"turn/start", "thread/start", "turn/start"}
	if fmt.Sprint(methods) != fmt.Sprint(want) {
		t.Fatalf("methods = %v, want %v", methods, want)
	}
}

func TestAppServerSessionSend_DoesNotRecoverOtherTurnStartErrors(t *testing.T) {
	var s *appServerSession
	var mu sync.Mutex
	var methods []string
	stdin := &callbackWriteCloser{onWrite: func(p []byte) {
		var request map[string]any
		if err := json.Unmarshal(bytes.TrimSpace(p), &request); err != nil {
			panic(fmt.Sprintf("decode request: %v", err))
		}
		id := int64(request["id"].(float64))
		method, _ := request["method"].(string)
		mu.Lock()
		methods = append(methods, method)
		mu.Unlock()
		s.handleResponse(rpcResponseEnvelope{ID: id, Error: &rpcError{Message: "rate limit exceeded"}})
	}}
	s = newScriptedAppServerSession(t, stdin)
	s.threadID.Store("thread-old")

	err := s.Send("continue the review", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "rate limit exceeded") {
		t.Fatalf("Send() error = %v, want original error", err)
	}
	if !s.Alive() {
		t.Fatal("session marked unhealthy for unrelated turn/start error")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(methods) != 1 || methods[0] != "turn/start" {
		t.Fatalf("methods = %v, want one turn/start", methods)
	}
}

func newScriptedAppServerSession(t *testing.T, stdin io.WriteCloser) *appServerSession {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	s := &appServerSession{
		workDir:       t.TempDir(),
		events:        make(chan core.Event, 16),
		ctx:           ctx,
		cancel:        cancel,
		stdin:         stdin,
		pending:       make(map[int64]chan rpcResponseEnvelope),
		functionCalls: make(map[string]string),
	}
	s.alive.Store(true)
	return s
}

var _ interface {
	GetUsage(context.Context) (*core.UsageReport, error)
} = (*appServerSession)(nil)

var _ interface {
	GetContextUsage() *core.ContextUsage
} = (*appServerSession)(nil)

type lockedWriteCloser struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

type callbackWriteCloser struct {
	onWrite func([]byte)
}

func (w *callbackWriteCloser) Write(p []byte) (int, error) {
	if w.onWrite != nil {
		w.onWrite(append([]byte(nil), p...))
	}
	return len(p), nil
}

func (w *callbackWriteCloser) Close() error { return nil }

var _ io.WriteCloser = (*callbackWriteCloser)(nil)

func (w *lockedWriteCloser) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *lockedWriteCloser) Close() error { return nil }

func (w *lockedWriteCloser) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

var _ io.WriteCloser = (*lockedWriteCloser)(nil)

type blockingWriteCloser struct {
	started   chan struct{}
	closed    chan struct{}
	closeOnce sync.Once

	mu       sync.Mutex
	isClosed bool
}

func newBlockingWriteCloser() *blockingWriteCloser {
	return &blockingWriteCloser{
		started: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (w *blockingWriteCloser) Write([]byte) (int, error) {
	select {
	case <-w.started:
	default:
		close(w.started)
	}
	<-w.closed
	return 0, io.ErrClosedPipe
}

func (w *blockingWriteCloser) Close() error {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.isClosed = true
		w.mu.Unlock()
		close(w.closed)
	})
	return nil
}

func (w *blockingWriteCloser) Closed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.isClosed
}

var _ io.WriteCloser = (*blockingWriteCloser)(nil)

func serverRequestProbe(t *testing.T, idJSON, method string, params any) map[string]json.RawMessage {
	t.Helper()
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	methodJSON, err := json.Marshal(method)
	if err != nil {
		t.Fatalf("marshal method: %v", err)
	}
	return map[string]json.RawMessage{
		"id":     json.RawMessage(idJSON),
		"method": methodJSON,
		"params": paramsJSON,
	}
}

func waitForWrittenJSONLine(t *testing.T, w *lockedWriteCloser) string {
	t.Helper()
	deadline := time.After(time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for JSON response, buffer=%q", w.String())
		case <-ticker.C:
			for _, line := range strings.Split(w.String(), "\n") {
				line = strings.TrimSpace(line)
				if line != "" {
					return line
				}
			}
		}
	}
}
