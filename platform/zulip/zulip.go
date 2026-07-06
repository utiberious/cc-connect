// Package zulip implements the core.Platform interface for Zulip.
//
// Design summary:
//   - Inbound events arrive via Zulip's REST event queue: POST /register,
//     then a long-poll loop on GET /events. See
//     https://zulip.com/api/register-queue and /api/get-events.
//   - Outbound messages use POST /messages (stream+topic or private DM) and
//     PATCH /messages/{id} for streaming-preview edits.
//   - SessionKey scoping mirrors the Discord platform:
//       thread_isolation=true (default)  ->  zulip:stream:{stream}:{topic}
//       thread_isolation=false            ->  zulip:stream:{stream}:{topic}:{user_id}
//       DMs                              ->  zulip:dm:{sender_email}
//     Topics ARE Zulip's thread primitive; treating topic as the isolation key
//     matches operator expectations and lines up with cc-connect's Discord
//     thread_isolation semantics.
//   - Group chatmode:
//       "oncall" (default) - only messages that mention the bot trigger a turn
//       "all"              - every stream message triggers a turn
//     DMs always trigger a turn (subject to dm_policy + allow_from).
package zulip

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

func init() {
	core.RegisterPlatform("zulip", New)
}

// ── Config ───────────────────────────────────────────────────────────────────

type Platform struct {
	baseURL         string
	email           string
	apiKey          string
	authHeader      string
	allowFrom       string   // comma-separated sender emails or "*"
	streams         []string // allowlist of stream names; ["*"] = all
	streamsAll      bool
	dmPolicy        string // "open" | "allowlist" | "closed"
	chatmode        string // "oncall" | "all" — for stream messages
	ackReaction     string // emoji name for inbound ack (empty = disabled)
	threadIsolation bool
	http            *http.Client

	handler core.MessageHandler
	cancel  context.CancelFunc

	// Filled in Start() after fetching /users/me.
	botUserID   atomic.Int64
	botEmail    atomic.Value // string
	botFullName atomic.Value // string
}

func New(opts map[string]any) (core.Platform, error) {
	baseURL, _ := opts["url"].(string)
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		return nil, errors.New("zulip: url is required")
	}
	email, _ := opts["email"].(string)
	apiKey, _ := opts["api_key"].(string)
	if apiKey == "" {
		// Accept the openclaw-plugin spelling too so config copy-paste works.
		apiKey, _ = opts["apiKey"].(string)
	}
	if email == "" || apiKey == "" {
		return nil, errors.New("zulip: email and api_key are required")
	}

	allowFrom, _ := opts["allow_from"].(string)
	dmPolicy, _ := opts["dm_policy"].(string)
	if dmPolicy == "" {
		dmPolicy = "open"
	}
	chatmode, _ := opts["chatmode"].(string)
	if chatmode == "" {
		chatmode = "oncall"
	}
	ackReaction, _ := opts["ack_reaction"].(string)

	threadIso := true
	if v, ok := opts["thread_isolation"].(bool); ok {
		threadIso = v
	}

	var streams []string
	streamsAll := false
	if raw, ok := opts["streams"].([]any); ok {
		for _, s := range raw {
			if str, ok := s.(string); ok && str != "" {
				if str == "*" {
					streamsAll = true
				}
				streams = append(streams, str)
			}
		}
	}
	if len(streams) == 0 {
		streamsAll = true
	}

	auth := base64.StdEncoding.EncodeToString([]byte(email + ":" + apiKey))

	// Long-poll uses a 90s server timeout by default (see registerQueue()),
	// so give the client a comfortable margin.
	client := &http.Client{Timeout: 120 * time.Second}

	if proxyURL, _ := opts["proxy"].(string); proxyURL != "" {
		u, err := url.Parse(proxyURL)
		if err != nil {
			return nil, fmt.Errorf("zulip: invalid proxy URL %q: %w", proxyURL, err)
		}
		if user, _ := opts["proxy_username"].(string); user != "" {
			pass, _ := opts["proxy_password"].(string)
			u.User = url.UserPassword(user, pass)
		}
		client.Transport = &http.Transport{Proxy: http.ProxyURL(u)}
	}

	return &Platform{
		baseURL:         baseURL,
		email:           email,
		apiKey:          apiKey,
		authHeader:      auth,
		allowFrom:       allowFrom,
		streams:         streams,
		streamsAll:      streamsAll,
		dmPolicy:        dmPolicy,
		chatmode:        chatmode,
		ackReaction:     ackReaction,
		threadIsolation: threadIso,
		http:            client,
	}, nil
}

func (p *Platform) Name() string { return "zulip" }

// ── REST helpers ─────────────────────────────────────────────────────────────

// apiCall issues a request against /api/v1<path>. If body is non-nil it is
// encoded as application/x-www-form-urlencoded. The response body is JSON-
// decoded into out. A non-2xx response is returned as an error whose text
// includes any msg field from the Zulip payload.
func (p *Platform) apiCall(ctx context.Context, method, path string, body url.Values, out any) error {
	return p.apiCallRaw(ctx, method, path, body, out, "")
}

func (p *Platform) apiCallRaw(ctx context.Context, method, path string, body url.Values, out any, contentType string) error {
	full := p.baseURL + "/api/v1" + path
	var reader io.Reader
	if body != nil {
		reader = strings.NewReader(body.Encode())
	}
	req, err := http.NewRequestWithContext(ctx, method, full, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Basic "+p.authHeader)
	if body != nil {
		if contentType == "" {
			contentType = "application/x-www-form-urlencoded"
		}
		req.Header.Set("Content-Type", contentType)
	}
	res, err := p.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode/100 != 2 {
		var errPayload struct {
			Result string `json:"result"`
			Msg    string `json:"msg"`
			Code   string `json:"code"`
		}
		_ = json.Unmarshal(raw, &errPayload)
		msg := errPayload.Msg
		if msg == "" {
			msg = strings.TrimSpace(string(raw))
		}
		return fmt.Errorf("zulip %s %s: %d %s: %s", method, path, res.StatusCode, res.Status, msg)
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("zulip %s %s: decode: %w", method, path, err)
		}
	}
	return nil
}

type meResponse struct {
	Result   string `json:"result"`
	Msg      string `json:"msg"`
	UserID   int64  `json:"user_id"`
	Email    string `json:"email"`
	FullName string `json:"full_name"`
}

type registerResponse struct {
	Result      string `json:"result"`
	Msg         string `json:"msg"`
	QueueID     string `json:"queue_id"`
	LastEventID int64  `json:"last_event_id"`
}

type eventsResponse struct {
	Result string  `json:"result"`
	Msg    string  `json:"msg"`
	Code   string  `json:"code"`
	Events []event `json:"events"`
}

type event struct {
	ID      int64   `json:"id"`
	Type    string  `json:"type"`
	Message *zulipMessage `json:"message,omitempty"`
}

type zulipMessage struct {
	ID              int64           `json:"id"`
	SenderID        int64           `json:"sender_id"`
	SenderEmail     string          `json:"sender_email"`
	SenderFullName  string          `json:"sender_full_name"`
	Content         string          `json:"content"`
	Timestamp       int64           `json:"timestamp"`
	Type            string          `json:"type"` // "stream" | "private"
	StreamID        int64           `json:"stream_id"`
	Subject         string          `json:"subject"`
	// display_recipient is a string for streams and an array for DMs; we
	// only need the stream case, so decode into a raw json.RawMessage and
	// pull the string out manually.
	DisplayRecipient json.RawMessage `json:"display_recipient"`
}

type sendResponse struct {
	Result string `json:"result"`
	Msg    string `json:"msg"`
	ID     int64  `json:"id"`
}

func (p *Platform) fetchMe(ctx context.Context) (userID int64, email, fullName string, err error) {
	var r meResponse
	if err = p.apiCall(ctx, http.MethodGet, "/users/me", nil, &r); err != nil {
		return
	}
	return r.UserID, r.Email, r.FullName, nil
}

func (p *Platform) registerQueue(ctx context.Context) (queueID string, lastEventID int64, err error) {
	body := url.Values{}
	body.Set("event_types", `["message"]`)
	body.Set("event_queue_longpoll_timeout_seconds", "90")
	var r registerResponse
	if err = p.apiCall(ctx, http.MethodPost, "/register", body, &r); err != nil {
		return
	}
	return r.QueueID, r.LastEventID, nil
}

func (p *Platform) getEvents(ctx context.Context, queueID string, lastEventID int64) ([]event, error) {
	q := url.Values{}
	q.Set("queue_id", queueID)
	q.Set("last_event_id", strconv.FormatInt(lastEventID, 10))
	q.Set("dont_block", "false")
	var r eventsResponse
	if err := p.apiCall(ctx, http.MethodGet, "/events?"+q.Encode(), nil, &r); err != nil {
		return nil, err
	}
	return r.Events, nil
}

func (p *Platform) deleteQueue(ctx context.Context, queueID string) {
	if queueID == "" {
		return
	}
	q := url.Values{}
	q.Set("queue_id", queueID)
	_ = p.apiCall(ctx, http.MethodDelete, "/events?"+q.Encode(), nil, nil)
}

func (p *Platform) sendStreamMessage(ctx context.Context, stream, topic, content string) (int64, error) {
	body := url.Values{}
	body.Set("type", "stream")
	body.Set("to", stream)
	body.Set("topic", topic)
	body.Set("content", content)
	var r sendResponse
	if err := p.apiCall(ctx, http.MethodPost, "/messages", body, &r); err != nil {
		return 0, err
	}
	return r.ID, nil
}

func (p *Platform) sendPrivateMessage(ctx context.Context, toEmail, content string) (int64, error) {
	body := url.Values{}
	body.Set("type", "private")
	// Zulip accepts a JSON array of recipients here.
	body.Set("to", `["`+toEmail+`"]`)
	body.Set("content", content)
	var r sendResponse
	if err := p.apiCall(ctx, http.MethodPost, "/messages", body, &r); err != nil {
		return 0, err
	}
	return r.ID, nil
}

func (p *Platform) editMessage(ctx context.Context, id int64, content string) error {
	body := url.Values{}
	body.Set("content", content)
	return p.apiCall(ctx, http.MethodPatch, "/messages/"+strconv.FormatInt(id, 10), body, nil)
}

func (p *Platform) addReaction(ctx context.Context, id int64, emojiName string) error {
	body := url.Values{}
	body.Set("emoji_name", emojiName)
	return p.apiCall(ctx, http.MethodPost, "/messages/"+strconv.FormatInt(id, 10)+"/reactions", body, nil)
}

// uploadFile posts a file to /user_uploads and returns the resulting url,
// which can then be embedded into a Zulip message body as
//
//	[filename](/user_uploads/...)
//
// to render as an inline attachment.
func (p *Platform) uploadFile(ctx context.Context, path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	var buf strings.Builder
	writer := multipart.NewWriter(&stringWriter{&buf})
	part, err := writer.CreateFormFile("file", filepath.Base(path))
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(part, f); err != nil {
		return "", err
	}
	if err := writer.Close(); err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/api/v1/user_uploads",
		strings.NewReader(buf.String()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Basic "+p.authHeader)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	res, err := p.http.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	if res.StatusCode/100 != 2 {
		return "", fmt.Errorf("zulip upload: %d %s: %s", res.StatusCode, res.Status, string(raw))
	}
	var parsed struct {
		Result string `json:"result"`
		Msg    string `json:"msg"`
		URI    string `json:"uri"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return "", err
	}
	if parsed.URI == "" {
		return "", errors.New("zulip upload: missing uri")
	}
	if strings.HasPrefix(parsed.URI, "/") {
		return p.baseURL + parsed.URI, nil
	}
	return parsed.URI, nil
}

// stringWriter is a tiny io.Writer over strings.Builder so we can hand a
// multipart body to multipart.NewWriter.
type stringWriter struct{ b *strings.Builder }

func (s *stringWriter) Write(p []byte) (int, error) { return s.b.Write(p) }

// ── Reply context ────────────────────────────────────────────────────────────

// replyContext carries whatever the outbound-side needs to Reply/Send to the
// same conversation the inbound message came from.
type replyContext struct {
	kind   string // "stream" | "private"
	stream string
	topic  string
	dmTo   string // email of the DM peer (for kind="private")
}

// ── Lifecycle ───────────────────────────────────────────────────────────────

func (p *Platform) Start(handler core.MessageHandler) error {
	p.handler = handler
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	userID, email, fullName, err := p.fetchMe(ctx)
	if err != nil {
		cancel()
		return fmt.Errorf("zulip: /users/me failed: %w", err)
	}
	p.botUserID.Store(userID)
	p.botEmail.Store(email)
	p.botFullName.Store(fullName)
	slog.Info("zulip: connected", "bot", fullName, "email", email, "user_id", userID)

	go p.pollLoop(ctx)
	return nil
}

func (p *Platform) pollLoop(ctx context.Context) {
	var queueID string
	var lastEventID int64 = -1
	backoff := time.Second
	const maxBackoff = 60 * time.Second

	defer func() {
		if queueID != "" {
			// Best-effort cleanup with a fresh context in case ctx is
			// already cancelled.
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			p.deleteQueue(cleanupCtx, queueID)
			cancel()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if queueID == "" {
			id, lastID, err := p.registerQueue(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Warn("zulip: /register failed", "error", err, "backoff", backoff)
				sleepCtx(ctx, backoff)
				backoff = min(backoff*2, maxBackoff)
				continue
			}
			queueID = id
			lastEventID = lastID
			backoff = time.Second
			slog.Info("zulip: event queue registered", "queue_id", queueID)
		}

		events, err := p.getEvents(ctx, queueID, lastEventID)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			msg := err.Error()
			// The event queue can expire; drop it and re-register.
			if strings.Contains(msg, "BAD_EVENT_QUEUE_ID") {
				slog.Info("zulip: queue expired, re-registering")
				queueID = ""
				continue
			}
			slog.Warn("zulip: /events failed", "error", err, "backoff", backoff)
			sleepCtx(ctx, backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}
		backoff = time.Second

		for _, ev := range events {
			if ev.ID > lastEventID {
				lastEventID = ev.ID
			}
			if ev.Type != "message" || ev.Message == nil {
				continue
			}
			p.processMessage(ctx, ev.Message)
		}
	}
}

func (p *Platform) processMessage(ctx context.Context, m *zulipMessage) {
	// Skip our own messages so we don't loop on our replies.
	if m.SenderID == p.botUserID.Load() {
		return
	}
	// Ignore stale messages that arrived while the process was down.
	if m.Timestamp > 0 && core.IsOldMessage(time.Unix(m.Timestamp, 0)) {
		return
	}

	isStream := m.Type == "stream" || m.Type == "channel"
	streamName := ""
	if isStream {
		// display_recipient is a plain string for streams.
		var s string
		if err := json.Unmarshal(m.DisplayRecipient, &s); err == nil {
			streamName = s
		}
	}
	topic := m.Subject
	if topic == "" {
		topic = "general"
	}

	// Allow-from check (sender email or "*").
	senderEmail := strings.ToLower(strings.TrimSpace(m.SenderEmail))
	if !core.AllowList(p.allowFrom, senderEmail) {
		slog.Debug("zulip: sender not on allow_from list", "sender", senderEmail)
		return
	}

	if isStream {
		if streamName == "" {
			return
		}
		if !p.streamAllowed(streamName) {
			return
		}
		if p.chatmode == "oncall" && !p.isMentioned(m.Content) {
			return
		}
	} else {
		// DM policy: "closed" always drops; "allowlist" relies on
		// allow_from (already checked above); "open" accepts.
		if p.dmPolicy == "closed" {
			return
		}
	}

	// Ack reaction (best-effort — never blocks the turn).
	if p.ackReaction != "" {
		go func(id int64) {
			bctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := p.addReaction(bctx, id, p.ackReaction); err != nil {
				slog.Debug("zulip: ack reaction failed", "error", err)
			}
		}(m.ID)
	}

	sessionKey := p.buildSessionKey(isStream, streamName, topic, senderEmail, m.SenderID)
	rctx := replyContext{}
	if isStream {
		rctx = replyContext{kind: "stream", stream: streamName, topic: topic}
	} else {
		rctx = replyContext{kind: "private", dmTo: senderEmail}
	}

	content := stripBotMention(m.Content, p.botFullName.Load().(string))

	coreMsg := &core.Message{
		SessionKey: sessionKey,
		Platform:   "zulip",
		UserID:     strconv.FormatInt(m.SenderID, 10),
		UserName:   coalesce(m.SenderFullName, senderEmail),
		Content:    content,
		ReplyCtx:   rctx,
	}

	slog.Debug("zulip: message received",
		"session", sessionKey, "sender", senderEmail, "type", m.Type)

	p.handler(p, coreMsg)
}

func (p *Platform) buildSessionKey(isStream bool, stream, topic, senderEmail string, senderID int64) string {
	if !isStream {
		return "zulip:dm:" + senderEmail
	}
	base := "zulip:stream:" + stream + ":" + topic
	if p.threadIsolation {
		return base
	}
	return base + ":" + strconv.FormatInt(senderID, 10)
}

func (p *Platform) streamAllowed(name string) bool {
	if p.streamsAll {
		return true
	}
	for _, s := range p.streams {
		if strings.EqualFold(s, name) {
			return true
		}
	}
	return false
}

func (p *Platform) isMentioned(content string) bool {
	full := strings.ToLower(p.botFullName.Load().(string))
	if full == "" {
		return false
	}
	c := strings.ToLower(content)
	// Zulip renders mentions as @**Full Name** and @**Full Name|user_id**;
	// after HTML → text extraction both reduce to the same substring.
	return strings.Contains(c, "@**"+full+"**") || strings.Contains(c, "@**"+full+"|")
}

func stripBotMention(content, botFullName string) string {
	if botFullName == "" {
		return strings.TrimSpace(content)
	}
	patterns := []string{
		"@**" + botFullName + "**",
		"@_**" + botFullName + "**", // silent mention
	}
	for _, pat := range patterns {
		content = strings.ReplaceAll(content, pat, "")
	}
	// Also strip @**Name|user_id** variants.
	if idx := strings.Index(content, "@**"+botFullName+"|"); idx >= 0 {
		end := strings.Index(content[idx:], "**")
		if end > 0 {
			content = content[:idx] + content[idx+end+2:]
		}
	}
	return strings.TrimSpace(content)
}

func coalesce(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}

// ── Outbound Platform methods ────────────────────────────────────────────────

// Reply is called for direct replies to an inbound message. Zulip has no
// per-message quote-reply on send, so Reply and Send are behaviourally
// identical.
func (p *Platform) Reply(ctx context.Context, rctx any, content string) error {
	return p.send(ctx, rctx, content)
}

func (p *Platform) Send(ctx context.Context, rctx any, content string) error {
	return p.send(ctx, rctx, content)
}

func (p *Platform) send(ctx context.Context, rctx any, content string) error {
	rc, ok := rctx.(replyContext)
	if !ok {
		return fmt.Errorf("zulip: invalid reply context type %T", rctx)
	}
	switch rc.kind {
	case "stream":
		_, err := p.sendStreamMessage(ctx, rc.stream, rc.topic, content)
		return err
	case "private":
		_, err := p.sendPrivateMessage(ctx, rc.dmTo, content)
		return err
	default:
		return fmt.Errorf("zulip: unknown reply context kind %q", rc.kind)
	}
}

// ── Streaming preview (MessageUpdater + optional SendPreviewStart) ──────────

type previewHandle struct {
	messageID int64
}

// SendPreviewStart posts an initial message and returns a handle that
// UpdateMessage can edit in place. Used by progress_style="card".
func (p *Platform) SendPreviewStart(ctx context.Context, rctx any, content string) (any, error) {
	rc, ok := rctx.(replyContext)
	if !ok {
		return nil, fmt.Errorf("zulip: invalid reply context type %T", rctx)
	}
	var id int64
	var err error
	switch rc.kind {
	case "stream":
		id, err = p.sendStreamMessage(ctx, rc.stream, rc.topic, content)
	case "private":
		id, err = p.sendPrivateMessage(ctx, rc.dmTo, content)
	default:
		return nil, fmt.Errorf("zulip: unknown reply context kind %q", rc.kind)
	}
	if err != nil {
		return nil, err
	}
	return &previewHandle{messageID: id}, nil
}

// UpdateMessage edits a previously-sent message identified by a previewHandle.
func (p *Platform) UpdateMessage(ctx context.Context, handle any, content string) error {
	h, ok := handle.(*previewHandle)
	if !ok || h == nil || h.messageID == 0 {
		return fmt.Errorf("zulip: invalid preview handle %T", handle)
	}
	return p.editMessage(ctx, h.messageID, content)
}

// DeletePreviewMessage removes a stale preview so the caller can send a fresh
// one. Zulip's DELETE /messages/{id} succeeds when the message is our own.
func (p *Platform) DeletePreviewMessage(ctx context.Context, handle any) error {
	h, ok := handle.(*previewHandle)
	if !ok || h == nil || h.messageID == 0 {
		return fmt.Errorf("zulip: invalid preview handle %T", handle)
	}
	return p.apiCall(ctx, http.MethodDelete,
		"/messages/"+strconv.FormatInt(h.messageID, 10), nil, nil)
}

// ── StreamingCardPlatform ────────────────────────────────────────────────────
//
// Zulip natively supports message edits (PATCH /messages/{id}) with no
// per-edit visual noise beyond a small "(edited)" marker, so the engine's
// StreamingCard path (see core/engine.go around EventText/EventThinking/
// EventToolUse) works cleanly for us: one initial POST + N PATCHes as
// thinking / tool progress / tool results / answer accumulate into a single
// message body. This is what lets tool-using pi/codex turns render as a
// single edited card instead of splitting into thinking + N tools + N
// results + final answer.
//
// The engine builds the card content string via its own composer and calls
// Update() with the full body each time — implementations do NOT compose;
// they only render the final markdown into a Zulip message.

// zulipStreamingCard implements core.StreamingCard by editing a single Zulip
// message in place as the engine accumulates content over the turn.
type zulipStreamingCard struct {
	platform    *Platform
	replyCtx    replyContext
	mu          sync.Mutex
	messageID   int64  // 0 until first Update() posts the initial message
	lastContent string // dedup: skip PATCHes that would send identical content
	failed      bool
}

func (c *zulipStreamingCard) sendInitial(ctx context.Context, content string) (int64, error) {
	switch c.replyCtx.kind {
	case "stream":
		return c.platform.sendStreamMessage(ctx, c.replyCtx.stream, c.replyCtx.topic, content)
	case "private":
		return c.platform.sendPrivateMessage(ctx, c.replyCtx.dmTo, content)
	default:
		return 0, fmt.Errorf("zulip streaming card: unknown replyCtx kind %q", c.replyCtx.kind)
	}
}

// Update replaces the card body with `content`. First call POSTs a new
// message; subsequent calls PATCH it.
func (c *zulipStreamingCard) Update(ctx context.Context, content string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed {
		return errors.New("zulip streaming card: previously failed")
	}
	if content == "" || content == c.lastContent {
		return nil
	}
	if c.messageID == 0 {
		id, err := c.sendInitial(ctx, content)
		if err != nil {
			c.failed = true
			return err
		}
		c.messageID = id
	} else {
		if err := c.platform.editMessage(ctx, c.messageID, content); err != nil {
			c.failed = true
			return err
		}
	}
	c.lastContent = content
	return nil
}

// Finalize renders the final content. Zulip has no separate "final" concept,
// so this is just a last Update.
func (c *zulipStreamingCard) Finalize(ctx context.Context, content string) error {
	return c.Update(ctx, content)
}

// Failed reports whether an earlier Update returned an error; the engine
// uses this to decide whether to fall back to sending individual messages.
func (c *zulipStreamingCard) Failed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failed
}

// CreateStreamingCard makes Zulip implement core.StreamingCardPlatform.
// The engine calls this at the start of each turn so that thinking, tool
// progress, and answer text can all be folded into one edited message
// instead of a series of separate posts.
func (p *Platform) CreateStreamingCard(ctx context.Context, replyCtx any) (core.StreamingCard, error) {
	rc, ok := replyCtx.(replyContext)
	if !ok {
		return nil, fmt.Errorf("zulip: invalid reply context type %T", replyCtx)
	}
	return &zulipStreamingCard{platform: p, replyCtx: rc}, nil
}

// ── Session reconstruction (for cron) ────────────────────────────────────────

// ReconstructReplyCtx recovers a replyContext from a stored SessionKey so
// scheduled jobs can post into the originating conversation.
func (p *Platform) ReconstructReplyCtx(sessionKey string) (any, error) {
	if !strings.HasPrefix(sessionKey, "zulip:") {
		return nil, fmt.Errorf("zulip: not a zulip session key %q", sessionKey)
	}
	rest := strings.TrimPrefix(sessionKey, "zulip:")
	switch {
	case strings.HasPrefix(rest, "dm:"):
		return replyContext{kind: "private", dmTo: strings.TrimPrefix(rest, "dm:")}, nil
	case strings.HasPrefix(rest, "stream:"):
		// zulip:stream:<stream>:<topic>[:<user_id>]
		parts := strings.SplitN(strings.TrimPrefix(rest, "stream:"), ":", 3)
		if len(parts) < 2 {
			return nil, fmt.Errorf("zulip: malformed stream session key %q", sessionKey)
		}
		return replyContext{kind: "stream", stream: parts[0], topic: parts[1]}, nil
	default:
		return nil, fmt.Errorf("zulip: unknown session-key scope %q", sessionKey)
	}
}

// ── Stop ─────────────────────────────────────────────────────────────────────

func (p *Platform) Stop() error {
	if p.cancel != nil {
		p.cancel()
	}
	return nil
}

// ── util ────────────────────────────────────────────────────────────────────

func sleepCtx(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
