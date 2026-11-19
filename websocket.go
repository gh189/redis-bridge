package bridge

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/redis/go-redis/v9"
)

const (
	wsMaxBatch     = 128
	wsQueue        = 1024
	wsMaxBlocking  = 16
	wsPingInterval = 20 * time.Second
)

func (b *Bridge) WebSocketHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case !b.authorized(r):
			b.fail(w, http.StatusUnauthorized, "WRONGPASS invalid or missing auth token")
			return
		case b.opts.AuthToken == "" && !sameOrigin(r):
			b.fail(w, http.StatusForbidden, "ERR cross-origin WebSocket connections need an auth token")
			return
		case !strings.EqualFold(r.Header.Get("Upgrade"), "websocket"):
			w.Header().Set("Upgrade", "websocket")
			b.fail(w, http.StatusUpgradeRequired, "ERR this endpoint only accepts WebSocket connections")
			return
		}
		conn, rw, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			b.errors.Add(1)
			if conn != nil {
				conn.Close()
			}
			return
		}
		b.websockets.Add(1)
		defer b.websockets.Add(-1)

		b64 := wantBase64(r) || strings.EqualFold(r.URL.Query().Get("encoding"), "base64")
		c := &wsConn{b: b, conn: conn, in: rw.Reader, out: rw.Writer, base64: b64,
			blocking: make(chan struct{}, wsMaxBlocking)}
		c.serve()
	})
}

func sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	return err == nil && strings.EqualFold(u.Host, r.Host)
}

func isBlocking(args []any) bool {
	switch strings.ToUpper(args[0].(string)) {
	case "BLPOP", "BRPOP", "BRPOPLPUSH", "BLMOVE", "BLMPOP", "BZPOPMIN", "BZPOPMAX", "BZMPOP", "WAIT", "WAITAOF":
		return true
	case "XREAD", "XREADGROUP":
		for _, arg := range args[1:] {
			if strings.EqualFold(arg.(string), "BLOCK") {
				return true
			}
			if strings.EqualFold(arg.(string), "STREAMS") {
				break
			}
		}
	}
	return false
}

func isPubSub(args []any) bool {
	switch strings.ToUpper(args[0].(string)) {
	case "SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE":
		return true
	}
	return false
}

type wsCommand struct {
	id    json.RawMessage
	args  []any
	multi [][]any
	err   error
}

type wsConn struct {
	b      *Bridge
	conn   net.Conn
	in     *bufio.Reader
	base64 bool
	ctx    context.Context

	mu  sync.Mutex
	out *bufio.Writer

	blocking chan struct{}

	subs     *redis.PubSub
	barriers int

	subMu     sync.Mutex
	barrier   string
	confirmed chan int
}

func (c *wsConn) serve() {
	ctx, cancel := context.WithCancel(context.Background())
	c.ctx = ctx
	defer c.conn.Close()
	defer cancel()
	defer func() {
		if c.subs != nil {
			c.subs.Close()
			c.b.subscribers.Add(-1)
		}
	}()

	queue := make(chan wsCommand, wsQueue)
	go c.read(queue)
	go c.keepAlive()

	batch := make([]wsCommand, 0, wsMaxBatch)
	for first := range queue {
		batch = append(batch[:0], first)
	drain:
		for len(batch) < wsMaxBatch {
			select {
			case cmd, ok := <-queue:
				if !ok {
					break drain
				}
				batch = append(batch, cmd)
			default:
				break drain
			}
		}
		if c.run(batch) != nil {
			return
		}
	}
}

func (c *wsConn) read(queue chan<- wsCommand) {
	defer close(queue)
	limit := c.b.opts.MaxBodyBytes
	control := wsutil.ControlFrameHandler(c, ws.StateServerSide)
	rd := wsutil.Reader{Source: c.in, State: ws.StateServerSide, CheckUTF8: true, MaxFrameSize: limit, OnIntermediate: control}
	for {
		hdr, err := rd.NextFrame()
		if errors.Is(err, wsutil.ErrFrameTooLarge) {
			c.close(ws.StatusMessageTooBig, "message is too large")
		}
		if err != nil {
			return
		}
		if hdr.OpCode.IsControl() {
			if control(hdr, &rd) != nil {
				return
			}
			continue
		}
		data, err := io.ReadAll(io.LimitReader(&rd, limit+1))
		if err != nil {
			return
		}
		if int64(len(data)) > limit {
			c.close(ws.StatusMessageTooBig, "message is too large")
			return
		}
		select {
		case queue <- c.parse(data):
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *wsConn) parse(data []byte) wsCommand {
	var msg struct {
		ID    json.RawMessage `json:"id"`
		Cmd   []any           `json:"cmd"`
		Multi [][]any         `json:"multi"`
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&msg); err != nil {
		return wsCommand{id: msg.ID, err: errors.New(`ERR message must be JSON like {"id":1,"cmd":["GET","key"]}: ` + err.Error())}
	}
	if msg.Multi == nil {
		args, err := toArgs(msg.Cmd)
		if err == nil {
			err = c.check(args)
		}
		return wsCommand{id: msg.ID, args: args, err: err}
	}
	if msg.Cmd != nil {
		return wsCommand{id: msg.ID, err: errors.New(`ERR a message has "cmd" or "multi", not both`)}
	}
	multi := make([][]any, len(msg.Multi))
	for i, item := range msg.Multi {
		args, err := toArgs(item)
		if err == nil {
			err = c.check(args)
		}
		if err == nil && isPubSub(args) {
			err = fmt.Errorf("ERR command '%s' is not available in a transaction", strings.ToUpper(args[0].(string)))
		}
		if err != nil {
			return wsCommand{id: msg.ID, err: fmt.Errorf("%s (command #%d)", err, i+1)}
		}
		multi[i] = args
	}
	return wsCommand{id: msg.ID, multi: multi}
}

func (c *wsConn) check(args []any) error {
	return c.b.check(args, "WebSocket", wsStateful)
}

func (c *wsConn) run(batch []wsCommand) error {
	start := 0
	for i, cmd := range batch {
		alone := c.alone(cmd)
		if alone == nil {
			continue
		}
		if err := c.pipeline(batch[start:i]); err != nil {
			return err
		}
		if err := alone(cmd); err != nil {
			return err
		}
		start = i + 1
	}
	return c.pipeline(batch[start:])
}

func (c *wsConn) alone(cmd wsCommand) func(wsCommand) error {
	switch {
	case cmd.err != nil:
		return nil
	case cmd.multi != nil:
		return c.transaction
	case isPubSub(cmd.args):
		return c.pubsub
	case isBlocking(cmd.args):
		return c.block
	}
	return nil
}

func (c *wsConn) pipeline(batch []wsCommand) error {
	if len(batch) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(c.ctx, c.b.opts.CommandTimeout)
	defer cancel()

	pipe := c.b.rdb.Pipeline()
	sent := make([]*redis.Cmd, len(batch))
	for i, cmd := range batch {
		if cmd.err == nil {
			sent[i] = pipe.Do(ctx, cmd.args...)
			c.b.commands.Add(1)
		}
	}
	_, _ = pipe.Exec(ctx)

	replies := make([][]byte, len(batch))
	for i, cmd := range batch {
		if cmd.err != nil {
			replies[i] = c.reply(cmd.id, nil, cmd.err)
			continue
		}
		value, err := sent[i].Result()
		replies[i] = c.reply(cmd.id, value, err)
	}
	return c.send(replies...)
}

func (c *wsConn) block(cmd wsCommand) error {
	select {
	case c.blocking <- struct{}{}:
	default:
		return c.send(c.reply(cmd.id, nil, errors.New("ERR too many blocking commands are waiting on this connection")))
	}
	go func() {
		defer func() { <-c.blocking }()
		value, err := c.b.run(c.ctx, cmd.args...)
		_ = c.send(c.reply(cmd.id, value, err))
	}()
	return nil
}

func (c *wsConn) transaction(cmd wsCommand) error {
	results, err := c.b.execBatch(c.ctx, cmd.multi, true, c.base64)
	if err != nil {
		return c.send(c.reply(cmd.id, nil, err))
	}
	return c.send(c.result(cmd.id, results))
}

func (c *wsConn) pubsub(cmd wsCommand) error {
	kind := strings.ToLower(cmd.args[0].(string))
	names := make([]string, len(cmd.args)-1)
	for i, arg := range cmd.args[1:] {
		names[i] = arg.(string)
	}
	subscribe := kind == "subscribe" || kind == "psubscribe"
	switch {
	case subscribe && len(names) == 0:
		return c.send(c.reply(cmd.id, nil, fmt.Errorf("ERR wrong number of arguments for '%s' command", kind)))
	case c.subs == nil && !subscribe:
		return c.send(c.reply(cmd.id, int64(0), nil))
	case c.subs == nil:
		c.subs = c.b.rdb.Subscribe(c.ctx)
		c.b.subscribers.Add(1)
		go c.forward(c.subs.ChannelWithSubscriptions())
	}

	c.barriers++
	barrier := fmt.Sprintf("redis-bridge:barrier:%d", c.barriers)
	confirmed := make(chan int, 1)
	c.subMu.Lock()
	c.barrier, c.confirmed = barrier, confirmed
	c.subMu.Unlock()

	ctx, cancel := context.WithTimeout(c.ctx, c.b.opts.CommandTimeout)
	defer cancel()
	var err error
	switch kind {
	case "subscribe":
		err = c.subs.Subscribe(ctx, names...)
	case "psubscribe":
		err = c.subs.PSubscribe(ctx, names...)
	case "unsubscribe":
		err = c.subs.Unsubscribe(ctx, names...)
	case "punsubscribe":
		err = c.subs.PUnsubscribe(ctx, names...)
	}
	c.b.commands.Add(1)
	if err == nil {
		err = c.subs.PUnsubscribe(ctx, barrier)
	}
	var count int
	if err == nil {
		select {
		case count = <-confirmed:
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	return c.send(c.reply(cmd.id, int64(count), err))
}

func (c *wsConn) forward(events <-chan any) {
	for event := range events {
		switch m := event.(type) {
		case *redis.Subscription:
			c.subMu.Lock()
			if m.Kind == "punsubscribe" && m.Channel == c.barrier {
				c.confirmed <- m.Count
				c.barrier = ""
			}
			c.subMu.Unlock()
		case *redis.Message:
			_ = c.send(c.push(m))
		}
	}
}

func (c *wsConn) push(m *redis.Message) []byte {
	kind, data := "message", m.Payload
	if m.Pattern != "" {
		kind = "pmessage"
	}
	if c.base64 {
		data = base64.StdEncoding.EncodeToString([]byte(data))
	}
	out, _ := json.Marshal(struct {
		Push    string `json:"push"`
		Pattern string `json:"pattern,omitempty"`
		Channel string `json:"channel"`
		Data    string `json:"data"`
	}{kind, m.Pattern, m.Channel, data})
	return out
}

func (c *wsConn) reply(id json.RawMessage, value any, err error) []byte {
	if err != nil && !errors.Is(err, redis.Nil) {
		c.b.errors.Add(1)
		out, _ := json.Marshal(struct {
			ID    json.RawMessage `json:"id"`
			Error string          `json:"error"`
		}{id, err.Error()})
		return out
	}
	return c.result(id, encodeReply(value, c.base64))
}

func (c *wsConn) result(id json.RawMessage, result any) []byte {
	out, _ := json.Marshal(struct {
		ID     json.RawMessage `json:"id"`
		Result any             `json:"result"`
	}{id, result})
	return out
}

func (c *wsConn) send(replies ...[]byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, reply := range replies {
		if err := wsutil.WriteServerMessage(c.out, ws.OpText, reply); err != nil {
			return err
		}
	}
	return c.out.Flush()
}

func (c *wsConn) Write(frame []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.out.Write(frame); err != nil {
		return 0, err
	}
	return len(frame), c.out.Flush()
}

func (c *wsConn) keepAlive() {
	ping := time.NewTicker(wsPingInterval)
	defer ping.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.b.closing:
			c.close(ws.StatusGoingAway, "server is shutting down")
			return
		case <-ping.C:
			if _, err := c.Write(ws.CompiledPing); err != nil {
				return
			}
		}
	}
}

func (c *wsConn) close(code ws.StatusCode, reason string) {
	_ = c.conn.SetWriteDeadline(time.Now().Add(time.Second))
	frame := ws.NewCloseFrame(ws.NewCloseFrameBody(code, reason))
	c.mu.Lock()
	if ws.WriteFrame(c.out, frame) == nil {
		_ = c.out.Flush()
	}
	c.mu.Unlock()
	c.conn.Close()
}
