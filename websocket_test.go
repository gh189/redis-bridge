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
	c.ask(`{"id":7,"cmd":["SELECT",1]}`, `{"id":7,"error":"ERR command 'SELECT' is not available over WebSocket: `+whyWsConn+`"}`)
	c.ask(`{"id":8,"cmd":["BGSAVE"]}`, `{"id":8,"error":"ERR command 'BGSAVE' is not available over WebSocket: `+whyAdmin+`"}`)
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

func TestWebSocketTransaction(t *testing.T) {
	srv, _, _ := newTestServer(t, Options{})
	c, _ := wsDial(t, wsURL(srv.URL, ""))

	c.ask(`{"id":1,"multi":[["SET","n",1],["INCR","n"],["LPUSH","n","x"]]}`,
		`{"id":1,"result":[{"result":"OK"},{"result":2},{"error":"WRONGTYPE Operation against a key holding the wrong kind of value"}]}`)
	c.ask(`{"id":2,"multi":[]}`, `{"id":2,"result":[]}`)

	c.send(`{"id":3,"cmd":["SET","n",10]}`, `{"id":4,"multi":[["INCR","n"],["INCR","n"]]}`, `{"id":5,"cmd":["GET","n"]}`)
	for _, want := range []string{`{"id":3,"result":"OK"}`, `{"id":4,"result":[{"result":11},{"result":12}]}`, `{"id":5,"result":"12"}`} {
		if got, err := c.recv(); got != want {
			t.Errorf("got %s (%v), want %s", got, err, want)
		}
	}

	c.ask(`{"id":6,"multi":[["INCR","n"],["SET","x"]]}`, `{"id":6,"error":"EXECABORT Transaction discarded because of previous errors."}`)
	c.ask(`{"id":7,"cmd":["GET","n"]}`, `{"id":7,"result":"12"}`)

	c.ask(`{"id":8,"cmd":["MULTI"]}`, `{"id":8,"error":"ERR command 'MULTI' is not available over WebSocket: `+whyWsTx+`"}`)
	c.ask(`{"id":9,"multi":[["INCR","n"],["WATCH","n"]]}`,
		`{"id":9,"error":"ERR command 'WATCH' is not available over WebSocket: `+whyWsTx+` (command #2)"}`)
	c.ask(`{"id":10,"multi":[["SUBSCRIBE","ch"]]}`, `{"id":10,"error":"ERR command 'SUBSCRIBE' is not available in a transaction (command #1)"}`)
	c.ask(`{"id":11,"multi":[["INCR",["n"]]]}`, `{"id":11,"error":"ERR argument #2 must be a string, number or boolean (command #1)"}`)
	c.ask(`{"id":12,"cmd":["GET","n"],"multi":[["GET","n"]]}`, `{"id":12,"error":"ERR a message has \"cmd\" or \"multi\", not both"}`)
	c.ask(`{"id":13,"cmd":["GET","n"]}`, `{"id":13,"result":"12"}`)
}

func TestWebSocketSubscribe(t *testing.T) {
	srv, b, _ := newTestServer(t, Options{})
	c, _ := wsDial(t, wsURL(srv.URL, ""))
	publish := func(channel, msg string, receivers int) {
		t.Helper()
		code, body := do(t, "POST", srv.URL+"/redis/channels/"+channel, msg)
		expect(t, code, body, 200, fmt.Sprintf(`{"result":%d}`, receivers))
	}

	c.ask(`{"id":1,"cmd":["UNSUBSCRIBE"]}`, `{"id":1,"result":0}`)
	c.ask(`{"id":2,"cmd":["SUBSCRIBE"]}`, `{"id":2,"error":"ERR wrong number of arguments for 'subscribe' command"}`)
	c.ask(`{"id":3,"cmd":["SUBSCRIBE","news","alerts"]}`, `{"id":3,"result":2}`)
	c.ask(`{"id":4,"cmd":["psubscribe","news.*"]}`, `{"id":4,"result":3}`)
	if st := b.Stats(); st.Subscribers != 1 {
		t.Errorf("subscribers = %d, want 1", st.Subscribers)
	}

	publish("news", "line1\nline2", 1)
	if got, _ := c.recv(); got != `{"push":"message","channel":"news","data":"line1\nline2"}` {
		t.Errorf("message = %s", got)
	}
	c.send(`{"id":5,"cmd":["SUBSCRIBE","jobs"]}`, `{"id":6,"cmd":["PUBLISH","jobs","j1"]}`, `{"id":7,"cmd":["PUBLISH","news.tech","hi"]}`)
	want := map[string]bool{
		`{"id":5,"result":4}`:                             true,
		`{"id":6,"result":1}`:                             true,
		`{"id":7,"result":1}`:                             true,
		`{"push":"message","channel":"jobs","data":"j1"}`: true,
		`{"push":"pmessage","pattern":"news.*","channel":"news.tech","data":"hi"}`: true,
	}
	for range len(want) {
		got, err := c.recv()
		if !want[got] {
			t.Errorf("unexpected %s (%v)", got, err)
		}
		delete(want, got)
	}

	c.ask(`{"id":8,"cmd":["UNSUBSCRIBE","news","nope"]}`, `{"id":8,"result":3}`)
	publish("news", "gone", 0)
	c.ask(`{"id":9,"cmd":["UNSUBSCRIBE"]}`, `{"id":9,"result":1}`)
	c.ask(`{"id":10,"cmd":["PUNSUBSCRIBE"]}`, `{"id":10,"result":0}`)
	publish("news.tech", "gone", 0)
	c.ask(`{"id":11,"cmd":["SSUBSCRIBE","news"]}`, `{"id":11,"error":"ERR command 'SSUBSCRIBE' is not available over WebSocket: `+whyWsSharded+`"}`)

	c64, _ := wsDial(t, wsURL(srv.URL, "?encoding=base64"))
	c64.ask(`{"id":1,"cmd":["SUBSCRIBE","news"]}`, `{"id":1,"result":1}`)
	publish("news", "hello", 1)
	if got, _ := c64.recv(); got != `{"push":"message","channel":"news","data":"aGVsbG8="}` {
		t.Errorf("base64 message = %s", got)
	}

	c.conn.Close()
	c64.conn.Close()
	waitFor(t, "the subscriptions to be released", func() bool { return b.Stats().Subscribers == 0 })
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

	c.send(`{"id":1,"cmd":["GET","k"]}`, `{"id":2,"cmd":["GET","k"]}`, `{"id":3,"cmd":["SUBSCRIBE","news"]}`)
	for i := 1; i <= 3; i++ {
		if got, err := c.recv(); !strings.HasPrefix(got, fmt.Sprintf(`{"id":%d,"error":"`, i)) {
			t.Errorf("reply #%d: got %s (%v), want an error", i, got, err)
		}
	}
}

func TestWebSocketClose(t *testing.T) {
	srv, b, _ := newTestServer(t, Options{})
	c, _ := wsDial(t, wsURL(srv.URL, ""))
	c.ask(`{"id":1,"cmd":["SUBSCRIBE","news"]}`, `{"id":1,"result":1}`)

	b.Close()
	_, err := c.recv()
	var closed wsutil.ClosedError
	if !errors.As(err, &closed) || closed.Code != ws.StatusGoingAway {
		t.Errorf("got %v, want close code %d", err, ws.StatusGoingAway)
	}
	waitFor(t, "the connection to be released", func() bool {
		st := b.Stats()
		return st.WebSockets == 0 && st.Subscribers == 0
	})
}
