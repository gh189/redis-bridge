package bridge

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	whyTx     = "transactions need one connection; POST the commands to the multi-exec endpoint"
	whyPubSub = "GET the channels or patterns endpoint (Server-Sent Events)"
	whyConn   = "it changes connection state, which does not exist over HTTP"
	whyAdmin  = "server administration commands are disabled"

	whyWsTx      = "put the commands in the multi field of one message"
	whyWsConn    = "it changes connection state, and WebSocket commands share Redis connections"
	whyWsSharded = "sharded Pub/Sub is not supported; use SUBSCRIBE"
)

var stateful = map[string]string{
	"MULTI": whyTx, "EXEC": whyTx, "DISCARD": whyTx, "WATCH": whyTx, "UNWATCH": whyTx,
	"SUBSCRIBE": whyPubSub, "UNSUBSCRIBE": whyPubSub, "PSUBSCRIBE": whyPubSub, "PUNSUBSCRIBE": whyPubSub,
	"SSUBSCRIBE": whyPubSub, "SUNSUBSCRIBE": whyPubSub,
	"SELECT": whyConn, "AUTH": whyConn, "HELLO": whyConn, "QUIT": whyConn, "RESET": whyConn,
	"CLIENT": whyConn, "MONITOR": whyConn, "SYNC": whyConn, "PSYNC": whyConn,
}

var wsStateful = map[string]string{
	"MULTI": whyWsTx, "EXEC": whyWsTx, "DISCARD": whyWsTx, "WATCH": whyWsTx, "UNWATCH": whyWsTx,
	"SSUBSCRIBE": whyWsSharded, "SUNSUBSCRIBE": whyWsSharded,
	"SELECT": whyWsConn, "AUTH": whyWsConn, "HELLO": whyWsConn, "QUIT": whyWsConn, "RESET": whyWsConn,
	"CLIENT": whyWsConn, "MONITOR": whyWsConn, "SYNC": whyWsConn, "PSYNC": whyWsConn,
}

var admin = map[string]bool{
	"SHUTDOWN": true, "REPLICAOF": true, "SLAVEOF": true, "FAILOVER": true,
	"SAVE": true, "BGSAVE": true, "BGREWRITEAOF": true,
	"DEBUG": true, "MODULE": true, "ACL": true,
}

func (b *Bridge) checkCommand(args []any) error {
	return b.check(args, "HTTP", stateful)
}

func (b *Bridge) check(args []any, api string, rejected map[string]string) error {
	if len(args) == 0 {
		return errors.New("ERR empty command")
	}
	name := strings.ToUpper(args[0].(string))
	if why, ok := rejected[name]; ok {
		return fmt.Errorf("ERR command '%s' is not available over %s: %s", name, api, why)
	}
	if b.opts.AllowAdminCommands {
		return nil
	}
	configWrite := name == "CONFIG" && (len(args) < 2 || !strings.EqualFold(args[1].(string), "GET"))
	if admin[name] || configWrite {
		return fmt.Errorf("ERR command '%s' is not available over %s: %s", name, api, whyAdmin)
	}
	return nil
}

func (b *Bridge) serveRedis(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, b.opts.MaxBodyBytes)
	resource, name, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")

	switch {
	case resource == "" && name == "":
		if !b.allow(w, r, http.MethodPost) {
			return
		}
		var raw []any
		if !b.decodeBody(w, r, &raw) {
			return
		}
		args, err := toArgs(raw)
		if err == nil {
			err = b.checkCommand(args)
		}
		if err != nil {
			b.fail(w, http.StatusBadRequest, err.Error())
			return
		}
		b.reply(w, r, args...)
	case (resource == "pipeline" || resource == "multi-exec") && name == "":
		if b.allow(w, r, http.MethodPost) {
			b.runBatch(w, r, resource == "multi-exec")
		}
	case resource == "keys":
		b.serveKey(w, r, name)
	case resource == "channels":
		b.serveChannel(w, r, name)
	case resource == "patterns":
		if b.allow(w, r, http.MethodGet) {
			b.subscribe(w, r, true, name)
		}
	default:
		b.fail(w, http.StatusNotFound, "ERR no such endpoint")
	}
}

func (b *Bridge) serveKey(w http.ResponseWriter, r *http.Request, key string) {
	if !b.allow(w, r, http.MethodGet, http.MethodPut, http.MethodDelete) {
		return
	}
	if key == "" {
		b.fail(w, http.StatusBadRequest, "ERR a key is required")
		return
	}
	switch r.Method {
	case http.MethodGet:
		value, err := b.run(r.Context(), "GET", key)
		switch {
		case err == nil && value == nil:
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "ERR no such key"})
		case err != nil && strings.HasPrefix(err.Error(), "WRONGTYPE"):
			b.fail(w, http.StatusBadRequest, "WRONGTYPE the key does not hold a string; POST the command for its type instead")
		default:
			b.write(w, r, value, err)
		}
	case http.MethodPut:
		body, ok := b.readBody(w, r)
		if !ok {
			return
		}
		args := []any{"SET", key, body}
		if ttl := r.URL.Query().Get("ttl"); ttl != "" {
			args = append(args, "EX", ttl)
		}
		b.reply(w, r, args...)
	case http.MethodDelete:
		b.reply(w, r, "DEL", key)
	}
}

func (b *Bridge) serveChannel(w http.ResponseWriter, r *http.Request, channel string) {
	if !b.allow(w, r, http.MethodGet, http.MethodPost) {
		return
	}
	if r.Method == http.MethodGet {
		b.subscribe(w, r, false, channel)
		return
	}
	if channel == "" {
		b.fail(w, http.StatusBadRequest, "ERR a channel is required")
		return
	}
	if body, ok := b.readBody(w, r); ok {
		b.reply(w, r, "PUBLISH", channel, body)
	}
}

func (b *Bridge) allow(w http.ResponseWriter, r *http.Request, methods ...string) bool {
	for _, m := range methods {
		if r.Method == m {
			return true
		}
	}
	allowed := strings.Join(methods, ", ")
	w.Header().Set("Allow", allowed)
	b.fail(w, http.StatusMethodNotAllowed, "ERR use "+allowed)
	return false
}

func (b *Bridge) readBody(w http.ResponseWriter, r *http.Request) (string, bool) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		b.fail(w, http.StatusRequestEntityTooLarge, "ERR request body is too large")
		return "", false
	}
	return string(body), true
}

func (b *Bridge) run(ctx context.Context, args ...any) (any, error) {
	ctx, cancel := context.WithTimeout(ctx, b.opts.CommandTimeout)
	defer cancel()

	b.commands.Add(1)
	value, err := b.rdb.Do(ctx, args...).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return value, err
}

func (b *Bridge) reply(w http.ResponseWriter, r *http.Request, args ...any) {
	value, err := b.run(r.Context(), args...)
	b.write(w, r, value, err)
}

func (b *Bridge) write(w http.ResponseWriter, r *http.Request, value any, err error) {
	if err != nil {
		b.fail(w, errorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"result": encodeReply(value, wantBase64(r))})
}

func (b *Bridge) runBatch(w http.ResponseWriter, r *http.Request, atomic bool) {
	var raw [][]any
	if !b.decodeBody(w, r, &raw) {
		return
	}
	batch := make([][]any, len(raw))
	for i, item := range raw {
		args, err := toArgs(item)
		if err == nil {
			err = b.checkCommand(args)
		}
		if err != nil {
			b.fail(w, http.StatusBadRequest, fmt.Sprintf("%s (command #%d)", err, i+1))
			return
		}
		batch[i] = args
	}
	out, err := b.execBatch(r.Context(), batch, atomic, wantBase64(r))
	if err != nil {
		b.fail(w, errorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (b *Bridge) execBatch(ctx context.Context, batch [][]any, atomic, b64 bool) ([]map[string]any, error) {
	out := make([]map[string]any, len(batch))
	if len(batch) == 0 {
		return out, nil
	}
	ctx, cancel := context.WithTimeout(ctx, b.opts.CommandTimeout)
	defer cancel()

	pipe := b.rdb.Pipeline()
	if atomic {
		pipe = b.rdb.TxPipeline()
	}
	cmds := make([]*redis.Cmd, len(batch))
	for i, args := range batch {
		cmds[i] = pipe.Do(ctx, args...)
	}

	b.commands.Add(int64(len(cmds)))
	if _, err := pipe.Exec(ctx); err != nil {
		var replyErr redis.Error
		aborted := atomic && strings.HasPrefix(err.Error(), "EXECABORT")
		if !errors.As(err, &replyErr) && !errors.Is(err, redis.Nil) || aborted {
			return nil, err
		}
	}
	for i, cmd := range cmds {
		value, err := cmd.Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			b.errors.Add(1)
			out[i] = map[string]any{"error": err.Error()}
		} else {
			out[i] = map[string]any{"result": encodeReply(value, b64)}
		}
	}
	return out, nil
}

func (b *Bridge) subscribe(w http.ResponseWriter, r *http.Request, pattern bool, channel string) {
	if channel == "" {
		b.fail(w, http.StatusBadRequest, "ERR a channel or pattern is required")
		return
	}
	ctx := r.Context()
	var sub *redis.PubSub
	if pattern {
		sub = b.rdb.PSubscribe(ctx, channel)
	} else {
		sub = b.rdb.Subscribe(ctx, channel)
	}
	defer sub.Close()
	b.subscribers.Add(1)
	defer b.subscribers.Add(-1)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher := http.NewResponseController(w)

	events := sub.ChannelWithSubscriptions()
	keepAlive := time.NewTicker(20 * time.Second)
	defer keepAlive.Stop()
	for {
		var event string
		select {
		case <-ctx.Done():
			return
		case <-b.closing:
			return
		case <-keepAlive.C:
			event = ": keep-alive\n\n"
		case msg, ok := <-events:
			if !ok {
				return
			}
			switch m := msg.(type) {
			case *redis.Subscription:
				event = sseData(fmt.Sprintf("%s,%s,%d", m.Kind, m.Channel, m.Count))
			case *redis.Message:
				if m.Pattern != "" {
					event = sseData("pmessage," + m.Pattern + "," + m.Channel + "," + m.Payload)
				} else {
					event = sseData("message," + m.Channel + "," + m.Payload)
				}
			default:
				continue
			}
		}
		if _, err := io.WriteString(w, event); err != nil {
			return
		}
		if err := flusher.Flush(); err != nil {
			return
		}
	}
}

func sseData(payload string) string {
	return "data: " + strings.ReplaceAll(payload, "\n", "\ndata: ") + "\n\n"
}

func (b *Bridge) decodeBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	if err := dec.Decode(dst); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			b.fail(w, http.StatusRequestEntityTooLarge, "ERR request body is too large")
		} else {
			b.fail(w, http.StatusBadRequest, "ERR request body must be a JSON array: "+err.Error())
		}
		return false
	}
	return true
}

func toArgs(raw []any) ([]any, error) {
	args := make([]any, len(raw))
	for i, v := range raw {
		switch x := v.(type) {
		case string:
			args[i] = x
		case json.Number:
			args[i] = x.String()
		case bool:
			args[i] = fmt.Sprint(x)
		default:
			return nil, fmt.Errorf("ERR argument #%d must be a string, number or boolean", i+1)
		}
	}
	return args, nil
}

func wantBase64(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upstash-Encoding"), "base64")
}

func encodeReply(v any, b64 bool) any {
	switch x := v.(type) {
	case nil, int64, float64, bool:
		return x
	case string:
		if b64 && x != "OK" {
			return base64.StdEncoding.EncodeToString([]byte(x))
		}
		return x
	case []any:
		out := make([]any, len(x))
		for i := range x {
			out[i] = encodeReply(x[i], b64)
		}
		return out
	case map[any]any:
		out := make([]any, 0, 2*len(x))
		for k, val := range x {
			out = append(out, encodeReply(k, b64), encodeReply(val, b64))
		}
		return out
	case error:
		return x.Error()
	default:
		return fmt.Sprint(x)
	}
}

func errorStatus(err error) int {
	var replyErr redis.Error
	var netErr net.Error
	switch {
	case errors.As(err, &replyErr):
		return http.StatusBadRequest
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &netErr) && netErr.Timeout():
		return http.StatusGatewayTimeout
	default:
		return http.StatusBadGateway
	}
}
