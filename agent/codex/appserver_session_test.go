package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"
	"testing"

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

func newAppServerEventTestSession() *appServerSession {
	return &appServerSession{
		events:      make(chan core.Event, 16),
		currentTurn: "turn-1",
	}
}

func notifyAppServerTest(t *testing.T, s *appServerSession, method string, params any) {
	t.Helper()
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
	var buf bytes.Buffer
	s := &appServerSession{
		ctx:     context.Background(),
		stdin:   nopAppServerWriteCloser{Writer: &buf},
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
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &payload); err != nil {
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

type nopAppServerWriteCloser struct {
	io.Writer
}

func (nopAppServerWriteCloser) Close() error { return nil }

var _ interface {
	GetUsage(context.Context) (*core.UsageReport, error)
} = (*appServerSession)(nil)

var _ interface {
	GetContextUsage() *core.ContextUsage
} = (*appServerSession)(nil)
