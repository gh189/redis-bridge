package bridge

import (
	"context"
	"net/http"
	"strings"
	"time"
)

type Status struct {
	Name          string      `json:"name"`
	Version       string      `json:"version"`
	StartedAt     time.Time   `json:"started_at"`
	UptimeSeconds int64       `json:"uptime_seconds"`
	Ready         bool        `json:"ready"`
	Stats         Stats       `json:"stats"`
	Redis         RedisStatus `json:"redis"`
	System        Usage       `json:"system"`
}

type RedisStatus struct {
	Info  map[string]map[string]string `json:"info,omitempty"`
	Error string                       `json:"error,omitempty"`
}

func (b *Bridge) Ready(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	return b.rdb.Ping(ctx).Err() == nil
}

func (b *Bridge) Status(ctx context.Context) Status {
	st := Status{
		Name:          "redis-bridge",
		Version:       Version,
		StartedAt:     b.started,
		UptimeSeconds: int64(time.Since(b.started).Seconds()),
		Stats:         b.Stats(),
		System:        b.system.usage(),
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	raw, err := b.rdb.Info(ctx).Result()
	if err != nil {
		st.Redis.Error = err.Error()
	} else {
		st.Redis.Info = parseInfo(raw)
		st.Ready = true
	}
	return st
}

func (b *Bridge) LiveHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
}

func (b *Bridge) ReadyHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ready := b.Ready(r.Context())
		code := http.StatusOK
		if !ready {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, map[string]bool{"ready": ready})
	})
}

func (b *Bridge) StatusHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, b.Status(r.Context()))
	})
}

func parseInfo(raw string) map[string]map[string]string {
	sections := map[string]map[string]string{}
	var current map[string]string
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if name, ok := strings.CutPrefix(line, "# "); ok {
			current = map[string]string{}
			sections[strings.ToLower(name)] = current
		} else if key, value, ok := strings.Cut(line, ":"); ok && current != nil {
			current[key] = value
		}
	}
	return sections
}
