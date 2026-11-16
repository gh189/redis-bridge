package bridge

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

type wsClient struct {
	t    *testing.T
	conn net.Conn
	rw   io.ReadWriter
}

func wsURL(srvURL, query string) string {
	return "ws" + strings.TrimPrefix(srvURL, "http") + "/redis-ws" + query
}

func wsDial(t *testing.T, url string, header ...string) (*wsClient, int) {
	t.Helper()
	h := http.Header{}
	for i := 0; i+1 < len(header); i += 2 {
		h.Set(header[i], header[i+1])
	}
	conn, br, _, err := ws.Dialer{Header: ws.HandshakeHeaderHTTP(h)}.Dial(context.Background(), url)
	var status ws.StatusError
	if errors.As(err, &status) {
		return nil, int(status)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	var rd io.Reader = conn
	if br != nil {
		rd = br
	}
	return &wsClient{t: t, conn: conn, rw: struct {
		io.Reader
		io.Writer
	}{rd, conn}}, 0
}

func (c *wsClient) send(msgs ...string) {
	c.t.Helper()
	var buf bytes.Buffer
	for _, msg := range msgs {
		if err := wsutil.WriteClientText(&buf, []byte(msg)); err != nil {
			c.t.Fatal(err)
		}
	}
	if _, err := c.conn.Write(buf.Bytes()); err != nil {
		c.t.Fatal(err)
	}
}

func (c *wsClient) recv() (string, error) {
	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	data, err := wsutil.ReadServerText(c.rw)
	return string(data), err
}

func (c *wsClient) ask(msg, want string) {
	c.t.Helper()
	c.send(msg)
	got, err := c.recv()
	if err != nil || got != want {
		c.t.Errorf("%s: got %s (%v), want %s", msg, got, err, want)
	}
}

func TestWebSocket(t *testing.T) {
	srv, b, _ := newTestServer(t, Options{})
	c, _ := wsDial(t, wsURL(srv.URL, ""))

	c.ask(`{"id":1,"cmd":["SET","greeting","hello"]}`, `{"id":1,"result":"OK"}`)
	c.ask(`{"id":"a-2","cmd":["GET","greeting"]}`, `{"id":"a-2","result":"hello"}`)
	c.ask(`{"id":3,"cmd":["GET","missing"]}`, `{"id":3,"result":null}`)
	c.ask(`{"id":4,"cmd":["INCRBY","n",5]}`, `{"id":4,"result":5}`)
	c.ask(`{"id":5,"cmd":["LRANGE","none",0,-1]}`, `{"id":5,"result":[]}`)

	c.ask(`{"id":6,"cmd":["INCR","greeting"]}`, `{"id":6,"error":"ERR value is not an integer or out of range"}`)
	c.ask(`{"id":7,"cmd":["MULTI"]}`, `{"id":7,"error":"ERR command 'MULTI' is not available over HTTP: `+whyTx+`"}`)
	c.ask(`{"id":8,"cmd":["BGSAVE"]}`, `{"id":8,"error":"ERR command 'BGSAVE' is not available over HTTP: `+whyAdmin+`"}`)
	c.ask(`{"id":9}`, `{"id":9,"error":"ERR empty command"}`)
	c.ask(`{"id":10,"cmd":["GET",["k"]]}`, `{"id":10,"error":"ERR argument #2 must be a string, number or boolean"}`)
	c.send(`nope`)
	if got, _ := c.recv(); !strings.HasPrefix(got, `{"id":null,"error":"ERR message must be JSON`) {
		t.Errorf("bad JSON: got %s", got)
	}
	c.ask(`{"id":11,"cmd":["GET","greeting"]}`, `{"id":11,"result":"hello"}`)

	first := ws.MaskFrameInPlace(ws.NewFrame(ws.OpText, false, []byte(`{"id":12,"cmd":["GE`)))
	rest := ws.MaskFrameInPlace(ws.NewFrame(ws.OpContinuation, true, []byte(`T","greeting"]}`)))
	if ws.WriteFrame(c.conn, first) != nil || ws.WriteFrame(c.conn, rest) != nil {
		t.Fatal("write fragments")
	}
	if got, _ := c.recv(); got != `{"id":12,"result":"hello"}` {
		t.Errorf("fragmented message: got %s", got)
	}

	if st := b.Stats(); st.WebSockets != 1 || st.Commands != 8 || st.Errors != 6 {
		t.Errorf("stats = %+v, want 1 websocket, 8 commands, 6 errors", st)
	}
	c.conn.Close()
	waitFor(t, "the connection to be released", func() bool { return b.Stats().WebSockets == 0 })
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func TestWebSocketPipelining(t *testing.T) {
	srv, _, _ := newTestServer(t, Options{})
	c, _ := wsDial(t, wsURL(srv.URL, ""))

	const n = 300
	msgs := make([]string, n)
	for i := range msgs {
		msgs[i] = fmt.Sprintf(`{"id":%d,"cmd":["INCR","counter"]}`, i+1)
	}
	msgs[99] = `{"id":100,"cmd":["LPUSH","counter","x"]}`
	c.send(msgs...)

	count := 0
	for i := 1; i <= n; i++ {
		want := fmt.Sprintf(`{"id":%d,"error":"WRONGTYPE Operation against a key holding the wrong kind of value"}`, i)
		if i != 100 {
			count++
			want = fmt.Sprintf(`{"id":%d,"result":%d}`, i, count)
		}
		if got, err := c.recv(); err != nil || got != want {
			t.Fatalf("reply #%d: got %s (%v), want %s", i, got, err, want)
		}
	}
}

func TestWebSocketBlocking(t *testing.T) {
	srv, _, _ := newTestServer(t, Options{})
	c, _ := wsDial(t, wsURL(srv.URL, ""))

	c.send(`{"id":1,"cmd":["BLPOP","jobs",5]}`, `{"id":2,"cmd":["SET","k","v"]}`)
	if got, _ := c.recv(); got != `{"id":2,"result":"OK"}` {
		t.Fatalf("expected the SET reply first, got %s", got)
	}
	c.send(`{"id":3,"cmd":["RPUSH","jobs","job-1"]}`)
	got := map[string]bool{}
	for range 2 {
		reply, err := c.recv()
		if err != nil {
			t.Fatal(err)
		}
		got[reply] = true
	}
	if !got[`{"id":3,"result":1}`] || !got[`{"id":1,"result":["jobs","job-1"]}`] {
		t.Errorf("replies = %v", got)
	}

	msgs := make([]string, wsMaxBlocking+1)
	for i := range msgs {
		msgs[i] = fmt.Sprintf(`{"id":%d,"cmd":["BLPOP","empty",1]}`, i+10)
	}
	c.send(msgs...)
	want := fmt.Sprintf(`{"id":%d,"error":"ERR too many blocking commands are waiting on this connection"}`, wsMaxBlocking+10)
	if got, _ := c.recv(); got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestIsBlocking(t *testing.T) {
	cases := map[string]bool{
		"BLPOP q 0":                                true,
		"blmove a b LEFT RIGHT 0":                  true,
		"XREAD BLOCK 0 STREAMS s $":                true,
		"XREADGROUP GROUP g c block 5 STREAMS s >": true,
		"XREAD COUNT 2 STREAMS block 0":            false,
		"LPOP q":                                   false,
		"GET BLOCK":                                false,
	}
	for line, want := range cases {
		var args []any
		for _, f := range strings.Fields(line) {
			args = append(args, f)
		}
		if got := isBlocking(args); got != want {
			t.Errorf("isBlocking(%q) = %v, want %v", line, got, want)
		}
	}
}

func TestWebSocketHandshake(t *testing.T) {
	srv, _, _ := newTestServer(t, Options{})
	host := strings.TrimPrefix(srv.URL, "http://")

	code, body := do(t, "GET", srv.URL+"/redis-ws", "")
	expect(t, code, body, 426, `{"error":"ERR this endpoint only accepts WebSocket connections"}`)

	if _, status := wsDial(t, wsURL(srv.URL, ""), "Origin", "http://evil.example"); status != 403 {
		t.Errorf("cross-origin without a token: status %d, want 403", status)
	}
	if c, status := wsDial(t, wsURL(srv.URL, ""), "Origin", "http://"+host); c == nil {
		t.Errorf("same origin: status %d", status)
	}

	c, _ := wsDial(t, wsURL(srv.URL, "?encoding=base64"))
	c.ask(`{"id":1,"cmd":["SET","k","hello"]}`, `{"id":1,"result":"OK"}`)
	c.ask(`{"id":2,"cmd":["GET","k"]}`, `{"id":2,"result":"aGVsbG8="}`)
	c, _ = wsDial(t, wsURL(srv.URL, ""), "Upstash-Encoding", "base64")
	c.ask(`{"id":3,"cmd":["GET","k"]}`, `{"id":3,"result":"aGVsbG8="}`)
}

func TestWebSocketAuth(t *testing.T) {
	srv, _, _ := newTestServer(t, Options{AuthToken: "s3cret"})

	if _, status := wsDial(t, wsURL(srv.URL, "")); status != 401 {
		t.Errorf("no token: status %d, want 401", status)
	}
	if _, status := wsDial(t, wsURL(srv.URL, "?_token=wrong")); status != 401 {
		t.Errorf("wrong token: status %d, want 401", status)
	}
	c, _ := wsDial(t, wsURL(srv.URL, ""), "Authorization", "Bearer s3cret")
	c.ask(`{"id":1,"cmd":["PING"]}`, `{"id":1,"result":"PONG"}`)

	c, _ = wsDial(t, wsURL(srv.URL, "?_token=s3cret"), "Origin", "http://app.example")
	c.ask(`{"id":2,"cmd":["PING"]}`, `{"id":2,"result":"PONG"}`)
}

func TestWebSocketMessageLimit(t *testing.T) {
	srv, _, _ := newTestServer(t, Options{MaxBodyBytes: 64})
	c, _ := wsDial(t, wsURL(srv.URL, ""))

	c.ask(`{"id":1,"cmd":["SET","k","small"]}`, `{"id":1,"result":"OK"}`)
	c.send(`{"id":2,"cmd":["SET","k","` + strings.Repeat("x", 64) + `"]}`)
	_, err := c.recv()
	var closed wsutil.ClosedError
	if !errors.As(err, &closed) || closed.Code != ws.StatusMessageTooBig {
		t.Errorf("got %v, want close code %d", err, ws.StatusMessageTooBig)
	}
}

func TestWebSocketRedisDown(t *testing.T) {
	srv, _, mr := newTestServer(t, Options{})
	c, _ := wsDial(t, wsURL(srv.URL, ""))
	mr.Close()

	c.send(`{"id":1,"cmd":["GET","k"]}`, `{"id":2,"cmd":["GET","k"]}`)
	for i := 1; i <= 2; i++ {
		if got, err := c.recv(); !strings.HasPrefix(got, fmt.Sprintf(`{"id":%d,"error":"`, i)) {
			t.Errorf("reply #%d: got %s (%v), want an error", i, got, err)
		}
	}
}

func TestWebSocketClose(t *testing.T) {
	srv, b, _ := newTestServer(t, Options{})
	c, _ := wsDial(t, wsURL(srv.URL, ""))
	c.ask(`{"id":1,"cmd":["PING"]}`, `{"id":1,"result":"PONG"}`)

	b.Close()
	_, err := c.recv()
	var closed wsutil.ClosedError
	if !errors.As(err, &closed) || closed.Code != ws.StatusGoingAway {
		t.Errorf("got %v, want close code %d", err, ws.StatusGoingAway)
	}
	waitFor(t, "the connection to be released", func() bool { return b.Stats().WebSockets == 0 })
}
