package bridge

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const Version = "1.1.0"

type Options struct {
	AuthToken          string
	CommandTimeout     time.Duration
	MaxBodyBytes       int64
	AllowAdminCommands bool
}

type Stats struct {
	Commands    int64 `json:"commands"`
	Errors      int64 `json:"errors"`
	Subscribers int64 `json:"subscribers"`
	WebSockets  int64 `json:"websockets"`
}

type Bridge struct {
	rdb     redis.UniversalClient
	opts    Options
	started time.Time
	system  *sampler

	commands, errors, subscribers, websockets atomic.Int64

	closing   chan struct{}
	closeOnce sync.Once
}

func RedisOptions(addr string) *redis.Options {
	return &redis.Options{
		Addr:                  addr,
		Protocol:              2,
		ReadTimeout:           -1,
		ContextTimeoutEnabled: true,
	}
}

func New(rdb redis.UniversalClient, opts Options) *Bridge {
	if opts.CommandTimeout <= 0 {
		opts.CommandTimeout = 60 * time.Second
	}
	if opts.MaxBodyBytes <= 0 {
		opts.MaxBodyBytes = 64 << 20
	}
	return &Bridge{
		rdb:     rdb,
		opts:    opts,
		started: time.Now(),
		system:  &sampler{diskPath: "."},
		closing: make(chan struct{}),
	}
}

func (b *Bridge) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !b.authorized(r) {
			b.fail(w, http.StatusUnauthorized, "WRONGPASS invalid or missing auth token")
			return
		}
		b.serveRedis(w, r)
	})
}

func (b *Bridge) authorized(r *http.Request) bool {
	want := b.opts.AuthToken
	if want == "" {
		return true
	}
	got, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if got == "" {
		got = r.URL.Query().Get("_token")
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func (b *Bridge) Close() {
	b.closeOnce.Do(func() { close(b.closing) })
}

func (b *Bridge) Stats() Stats {
	return Stats{Commands: b.commands.Load(), Errors: b.errors.Load(),
		Subscribers: b.subscribers.Load(), WebSockets: b.websockets.Load()}
}

func (b *Bridge) fail(w http.ResponseWriter, code int, msg string) {
	b.errors.Add(1)
	writeJSON(w, code, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
