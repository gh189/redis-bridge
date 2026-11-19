package bridge

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestServer(t *testing.T, opts Options) (*httptest.Server, *Bridge, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(RedisOptions(mr.Addr()))
	t.Cleanup(func() { rdb.Close() })

	b := New(rdb, opts)
	mux := http.NewServeMux()
	redisAPI := http.StripPrefix("/redis", b.Handler())
	mux.Handle("/redis", redisAPI)
	mux.Handle("/redis/", redisAPI)
	mux.Handle("/redis-ws", b.WebSocketHandler())
	mux.Handle("/healthz", b.LiveHandler())
	mux.Handle("/readyz", b.ReadyHandler())
	mux.Handle("/status", b.StatusHandler())
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, b, mr
}

func do(t *testing.T, method, url, body string, header ...string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(raw))
}

func expect(t *testing.T, code int, body string, wantCode int, wantBody string) {
	t.Helper()
	if code != wantCode || body != wantBody {
		t.Errorf("got %d %s, want %d %s", code, body, wantCode, wantBody)
	}
}

func TestKeys(t *testing.T) {
	srv, b, _ := newTestServer(t, Options{})
	u := srv.URL + "/redis/keys/"

	code, body := do(t, "PUT", u+"greeting", "hello")
	expect(t, code, body, 200, `{"result":"OK"}`)
	code, body = do(t, "GET", u+"greeting", "")
	expect(t, code, body, 200, `{"result":"hello"}`)
	code, body = do(t, "GET", u+"greeting", "", "Upstash-Encoding", "base64")
	expect(t, code, body, 200, `{"result":"aGVsbG8="}`)
	code, body = do(t, "GET", u+"missing", "")
	expect(t, code, body, 404, `{"error":"ERR no such key"}`)

	code, body = do(t, "PUT", u+"a/b/c", "nested")
	expect(t, code, body, 200, `{"result":"OK"}`)
	code, body = do(t, "POST", srv.URL+"/redis", `["GET","a/b/c"]`)
	expect(t, code, body, 200, `{"result":"nested"}`)
	code, body = do(t, "GET", u+"a%2Fb%2Fc", "")
	expect(t, code, body, 200, `{"result":"nested"}`)
	code, body = do(t, "PUT", u+"user:1", "")
	expect(t, code, body, 200, `{"result":"OK"}`)
	code, body = do(t, "GET", u+"user:1", "")
	expect(t, code, body, 200, `{"result":""}`)

	code, body = do(t, "PUT", u+"temp?ttl=60", "1")
	expect(t, code, body, 200, `{"result":"OK"}`)
	code, body = do(t, "POST", srv.URL+"/redis", `["TTL","temp"]`)
	expect(t, code, body, 200, `{"result":60}`)

	code, body = do(t, "DELETE", u+"greeting", "")
	expect(t, code, body, 200, `{"result":1}`)
	code, body = do(t, "DELETE", u+"greeting", "")
	expect(t, code, body, 200, `{"result":0}`)
	code, _ = do(t, "GET", u+"greeting", "")
	expect(t, code, "", 404, "")
	if st := b.Stats(); st.Errors != 0 {
		t.Errorf("stats = %+v, want no errors so far", st)
	}

	code, _ = do(t, "PUT", u+"temp?ttl=soon", "1")
	expect(t, code, "", 400, "")
	do(t, "POST", srv.URL+"/redis", `["RPUSH","queue","job"]`)
	code, body = do(t, "GET", u+"queue", "")
	if code != 400 || !strings.Contains(body, "WRONGTYPE") {
		t.Errorf("GET on a list: %d %s", code, body)
	}
	code, _ = do(t, "GET", u, "")
	expect(t, code, "", 400, "")
	if st := b.Stats(); st.Errors != 3 {
		t.Errorf("stats = %+v, want 3 errors", st)
	}
}

func TestCommands(t *testing.T) {
	srv, b, _ := newTestServer(t, Options{})
	u := srv.URL + "/redis"

	code, body := do(t, "POST", u, `["HSET","user","name","Ada","age",36]`)
	expect(t, code, body, 200, `{"result":2}`)
	code, body = do(t, "POST", u+"/", `["HGET","user","age"]`)
	expect(t, code, body, 200, `{"result":"36"}`)
	code, body = do(t, "POST", u, `["GET","missing"]`)
	expect(t, code, body, 200, `{"result":null}`)

	code, _ = do(t, "POST", u, `["GET"]`)
	expect(t, code, "", 400, "")
	code, _ = do(t, "POST", u, `{"not":"an array"}`)
	expect(t, code, "", 400, "")
	code, _ = do(t, "POST", u, `["MULTI"]`)
	expect(t, code, "", 400, "")

	for _, path := range []string{"", "/pipeline", "/multi-exec"} {
		resp, err := http.Get(u + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 405 || resp.Header.Get("Allow") != "POST" {
			t.Errorf("GET %q: %d, Allow %q", path, resp.StatusCode, resp.Header.Get("Allow"))
		}
	}
	code, _ = do(t, "POST", u+"/keys/user", "x")
	expect(t, code, "", 405, "")
	code, _ = do(t, "GET", u+"/set/k/v", "")
	expect(t, code, "", 404, "")
	code, _ = do(t, "GET", u+"/get/user", "")
	expect(t, code, "", 404, "")
	code, body = do(t, "POST", u, `["EXISTS","k"]`)
	expect(t, code, body, 200, `{"result":0}`)

	if st := b.Stats(); st.Commands != 5 || st.Errors != 9 {
		t.Errorf("stats = %+v, want 5 commands and 9 errors", st)
	}
}

func TestBodyLimit(t *testing.T) {
	srv, _, _ := newTestServer(t, Options{MaxBodyBytes: 16})
	u := srv.URL + "/redis"

	code, body := do(t, "PUT", u+"/keys/k", "0123456789abcdef")
	expect(t, code, body, 200, `{"result":"OK"}`)
	code, _ = do(t, "PUT", u+"/keys/k", "0123456789abcdefg")
	expect(t, code, "", 413, "")
	code, _ = do(t, "POST", u, `["SET","k","0123456789"]`)
	expect(t, code, "", 413, "")
	code, body = do(t, "GET", u+"/keys/k", "")
	expect(t, code, body, 200, `{"result":"0123456789abcdef"}`)
}

func TestPipelineAndTransaction(t *testing.T) {
	srv, _, _ := newTestServer(t, Options{})
	u := srv.URL + "/redis"

	code, body := do(t, "POST", u+"/pipeline", `[["SET","n",1],["INCR","n"],["LPUSH","n","x"],["GET","nope"]]`)
	if code != 200 || !strings.HasPrefix(body, `[{"result":"OK"},{"result":2},{"error":"WRONGTYPE`) || !strings.HasSuffix(body, `{"result":null}]`) {
		t.Errorf("pipeline: %d %s", code, body)
	}
	code, body = do(t, "POST", u+"/multi-exec", `[["INCRBY","n",10],["RPUSH","orders","o1"]]`)
	expect(t, code, body, 200, `[{"result":12},{"result":1}]`)
	code, body = do(t, "POST", u+"/pipeline", `[["SET","x",1],["MULTI"]]`)
	if code != 400 || !strings.Contains(body, "command #2") {
		t.Errorf("pipeline with MULTI: %d %s", code, body)
	}
	code, body = do(t, "POST", u+"/pipeline", `[]`)
	expect(t, code, body, 200, `[]`)
}

func TestCheckCommand(t *testing.T) {
	strict, relaxed := New(nil, Options{}), New(nil, Options{AllowAdminCommands: true})
	allowed := [][]any{{"GET", "k"}, {"set", "k", "v"}, {"CONFIG", "GET", "maxmemory"}, {"FLUSHALL"}, {"BLPOP", "q", "5"}}
	for _, args := range allowed {
		if err := strict.checkCommand(args); err != nil {
			t.Errorf("checkCommand(%v) = %v, want nil", args, err)
		}
	}
	stateful := [][]any{{}, {"MULTI"}, {"exec"}, {"Subscribe", "ch"}, {"SELECT", "1"}, {"CLIENT", "LIST"}}
	for _, args := range stateful {
		if strict.checkCommand(args) == nil || relaxed.checkCommand(args) == nil {
			t.Errorf("checkCommand(%v) accepted a stateful command", args)
		}
	}
	admin := [][]any{{"SHUTDOWN"}, {"bgsave"}, {"CONFIG"}, {"CONFIG", "SET", "save", "60 1"}, {"ACL", "LIST"}}
	for _, args := range admin {
		if strict.checkCommand(args) == nil {
			t.Errorf("checkCommand(%v) accepted an admin command by default", args)
		}
		if err := relaxed.checkCommand(args); err != nil {
			t.Errorf("checkCommand(%v) with AllowAdminCommands = %v", args, err)
		}
	}
}

func TestAuth(t *testing.T) {
	srv, _, _ := newTestServer(t, Options{AuthToken: "s3cret"})
	u := srv.URL + "/redis/keys/k"

	code, _ := do(t, "PUT", u, "v")
	expect(t, code, "", 401, "")
	code, _ = do(t, "PUT", u, "v", "Authorization", "Bearer wrong")
	expect(t, code, "", 401, "")
	code, body := do(t, "PUT", u, "v", "Authorization", "Bearer s3cret")
	expect(t, code, body, 200, `{"result":"OK"}`)
	code, body = do(t, "GET", u+"?_token=s3cret", "")
	expect(t, code, body, 200, `{"result":"v"}`)
	code, _ = do(t, "GET", srv.URL+"/healthz", "")
	expect(t, code, "", 200, "")
}

func TestSubscribe(t *testing.T) {
	srv, b, _ := newTestServer(t, Options{})
	resp, err := http.Get(srv.URL + "/redis/channels/news")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	lines := make(chan string, 16)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			if scanner.Text() != "" {
				lines <- scanner.Text()
			}
		}
	}()
	next := func() string {
		select {
		case line, ok := <-lines:
			if !ok {
				return "<closed>"
			}
			return line
		case <-time.After(5 * time.Second):
			return "<timeout>"
		}
	}

	if got := next(); got != "data: subscribe,news,1" {
		t.Fatalf("first event = %q", got)
	}
	code, body := do(t, "POST", srv.URL+"/redis/channels/news", "line1\nline2")
	expect(t, code, body, 200, `{"result":1}`)
	if got := next(); got != "data: message,news,line1" {
		t.Errorf("message = %q", got)
	}
	if got := next(); got != "data: line2" {
		t.Errorf("second line = %q", got)
	}
	if b.Stats().Subscribers != 1 {
		t.Errorf("subscribers = %d, want 1", b.Stats().Subscribers)
	}

	b.Close()
	if got := next(); got != "<closed>" {
		t.Errorf("after Close: %q", got)
	}
}

func TestPatternSubscribe(t *testing.T) {
	srv, _, _ := newTestServer(t, Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/redis/patterns/news.*", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	next := func() string {
		for scanner.Scan() {
			if scanner.Text() != "" {
				return scanner.Text()
			}
		}
		return "<closed>"
	}

	if got := next(); got != "data: psubscribe,news.*,1" {
		t.Fatalf("first event = %q", got)
	}
	code, body := do(t, "POST", srv.URL+"/redis/channels/news.tech", "hi")
	expect(t, code, body, 200, `{"result":1}`)
	if got := next(); got != "data: pmessage,news.*,news.tech,hi" {
		t.Errorf("message = %q", got)
	}

	code, _ = do(t, "POST", srv.URL+"/redis/patterns/news.*", "hi")
	expect(t, code, "", 405, "")
	code, _ = do(t, "GET", srv.URL+"/redis/channels/", "")
	expect(t, code, "", 400, "")
}

func TestProbesAndStatus(t *testing.T) {
	srv, _, mr := newTestServer(t, Options{})

	code, body := do(t, "GET", srv.URL+"/healthz", "")
	expect(t, code, body, 200, `{"status":"ok"}`)
	code, body = do(t, "GET", srv.URL+"/readyz", "")
	expect(t, code, body, 200, `{"ready":true}`)

	do(t, "PUT", srv.URL+"/redis/keys/k", "v")
	code, body = do(t, "GET", srv.URL+"/status", "")
	var st Status
	if err := json.Unmarshal([]byte(body), &st); err != nil || code != 200 {
		t.Fatalf("status: %d %s (%v)", code, body, err)
	}
	if st.Version != Version || !st.Ready || st.Stats.Commands != 1 || st.Redis.Info == nil || st.System.MemoryTotal == 0 {
		t.Errorf("unexpected status: %+v", st)
	}

	mr.Close()
	code, body = do(t, "GET", srv.URL+"/readyz", "")
	expect(t, code, body, 503, `{"ready":false}`)
	code, _ = do(t, "GET", srv.URL+"/redis/keys/k", "")
	expect(t, code, "", 502, "")
}

func TestToArgs(t *testing.T) {
	got, err := toArgs([]any{"SET", "k", json.Number("1.50"), true})
	want := []any{"SET", "k", "1.50", "true"}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("toArgs = %v, %v; want %v", got, err, want)
	}
	for _, bad := range []any{nil, []any{"x"}, map[string]any{}} {
		if _, err := toArgs([]any{"SET", "k", bad}); err == nil {
			t.Errorf("toArgs accepted %#v", bad)
		}
	}
}

func TestEncodeReply(t *testing.T) {
	reply := []any{"OK", "bar", int64(7), nil, []any{"x"}, errors.New("ERR nested"), map[any]any{"k": "v"}}
	plain := []any{"OK", "bar", int64(7), nil, []any{"x"}, "ERR nested", []any{"k", "v"}}
	if got := encodeReply(reply, false); !reflect.DeepEqual(got, plain) {
		t.Errorf("plain = %v, want %v", got, plain)
	}
	b64 := []any{"OK", "YmFy", int64(7), nil, []any{"eA=="}, "ERR nested", []any{"aw==", "dg=="}}
	if got := encodeReply(reply, true); !reflect.DeepEqual(got, b64) {
		t.Errorf("base64 = %v, want %v", got, b64)
	}
}

func TestParseInfo(t *testing.T) {
	got := parseInfo("# Server\r\nredis_version:8.10.3\r\n\r\n# Keyspace\r\ndb0:keys=3,expires=0\r\n")
	want := map[string]map[string]string{
		"server":   {"redis_version": "8.10.3"},
		"keyspace": {"db0": "keys=3,expires=0"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("parseInfo = %v, want %v", got, want)
	}
}
