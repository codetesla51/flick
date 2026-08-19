package main

import (
	"embed"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/codetesla51/flick"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed dashboard.html
var dashboardFS embed.FS

// flagRecord is the JSON shape returned by GET /api/flags.
type flagRecord struct {
	Key            string          `json:"key"`
	State          string          `json:"state"`
	DefaultVariant string          `json:"defaultVariant"`
	Variants       json.RawMessage `json:"variants"`
	Targeting      json.RawMessage `json:"targeting"`
	Metadata       json.RawMessage `json:"metadata"`
	UpdatedAt      time.Time       `json:"updatedAt"`
}

// writeJSON renders v as JSON, returning 500 on marshal failure.
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, `{"error":"encode response"}`, http.StatusInternalServerError)
	}
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// newDashboardMux serves the flick console: the embedded dashboard at /,
// the CRUD API under /api/flags, plus live SSE endpoints fed by the notify
// layer (events) and a per-second metrics stream (counters + pending).
func newDashboardMux(pool *pgxpool.Pool, layer *flick.NotifyLayer, hub *flick.Hub) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		body, err := dashboardFS.ReadFile("dashboard.html")
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "dashboard not embedded")
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(body)
	})

	// GET  /api/flags          → list all flags
	// POST /api/flags          → create or update (SetFlag semantics)
	// DELETE /api/flags/{key}  → delete
	mux.HandleFunc("/api/flags", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleListFlags(w, r, pool)
		case http.MethodPost:
			handleUpsertFlag(w, r, pool)
		default:
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
	mux.HandleFunc("/api/flags/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		key, err := url.PathUnescape(strings.TrimPrefix(r.URL.Path, "/api/flags/"))
		if err != nil || key == "" || strings.Contains(key, "/") {
			writeErr(w, http.StatusBadRequest, "invalid flag key")
			return
		}
		handleDeleteFlag(w, r, pool, key)
	})

	// Live streams, fed by the notify layer (events) and live counters
	// (metrics); the console HTML is unchanged.
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		handleEventsSSE(w, r, layer)
	})
	mux.HandleFunc("/metrics/stream", func(w http.ResponseWriter, r *http.Request) {
		handleMetricsSSE(w, r, layer, hub)
	})

	return mux
}

// handleEventsSSE streams flag-change events fed by the notify layer in a
// compact {Table, Operation, NewRow} shape. New clients get recent history
// first, then live events.
func handleEventsSSE(w http.ResponseWriter, r *http.Request, layer *flick.NotifyLayer) {
	fl, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	fl.Flush()

	id, ch := layer.SubscribeEvents()
	defer layer.UnsubscribeEvents(id)

	for {
		select {
		case <-r.Context().Done():
			return
		case e, ok := <-ch:
			if !ok {
				return
			}
			op := "insert"
			if deleted, _ := e.Payload["deleted"].(bool); deleted {
				op = "delete"
			}
			evt := map[string]any{
				"Table":     "flags",
				"Operation": op,
				"NewRow": map[string]any{
					"key":     e.Payload["key"],
					"payload": e.Payload,
				},
			}
			b, err := json.Marshal(evt)
			if err != nil {
				return
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
				return
			}
			fl.Flush()
		}
	}
}

// handleMetricsSSE streams the notify layer's counters every second.
func handleMetricsSSE(w http.ResponseWriter, r *http.Request, layer *flick.NotifyLayer, hub *flick.Hub) {
	fl, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	fl.Flush()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			m := layer.MetricsSnapshot()
			m.Subscribers = hub.SubscriberCount()
			b, err := json.Marshal(m)
			if err != nil {
				return
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
				return
			}
			fl.Flush()
		}
	}
}

func handleListFlags(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	rows, err := pool.Query(r.Context(), `
		SELECT key, state, default_variant, variants, targeting, metadata, updated_at
		FROM flags ORDER BY key`)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, fmt.Sprintf("query flags: %v", err))
		return
	}
	defer rows.Close()

	flags := []flagRecord{}
	for rows.Next() {
		var f flagRecord
		if err := rows.Scan(&f.Key, &f.State, &f.DefaultVariant, &f.Variants, &f.Targeting, &f.Metadata, &f.UpdatedAt); err != nil {
			writeErr(w, http.StatusInternalServerError, fmt.Sprintf("scan flag: %v", err))
			return
		}
		flags = append(flags, f)
	}
	if err := rows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, fmt.Sprintf("iterate flags: %v", err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"flags": flags})
}

// flagWrite is the request body for POST /api/flags.
type flagWrite struct {
	Key            string          `json:"key"`
	State          string          `json:"state"`
	DefaultVariant string          `json:"defaultVariant"`
	Variants       json.RawMessage `json:"variants"`
	Targeting      json.RawMessage `json:"targeting"`
	Metadata       json.RawMessage `json:"metadata"`
}

func handleUpsertFlag(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	var body flagWrite
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if body.Key == "" {
		writeErr(w, http.StatusBadRequest, "key is required")
		return
	}
	variants, targeting, metadata, err := validateFlagWrite(body.State, body.DefaultVariant, body.Variants, body.Targeting, body.Metadata)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	_ = variants // already validated; pass raw JSON to SetFlag

	targetingRaw, _ := json.Marshal(targeting)
	metadataRaw, _ := json.Marshal(metadata)
	if err := flick.SetFlag(r.Context(), pool, body.Key, body.State, body.DefaultVariant, body.Variants, targetingRaw, metadataRaw); err != nil {
		writeErr(w, http.StatusInternalServerError, fmt.Sprintf("set flag: %v", err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "key": body.Key})
}

func handleDeleteFlag(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool, key string) {
	// DeleteFlag is a no-op for absent keys (returns nil, no event enqueued).
	// We need to distinguish 200-ok from 404, so check existence first.
	var exists bool
	if err := pool.QueryRow(r.Context(),
		`SELECT EXISTS (SELECT 1 FROM flags WHERE key=$1)`, key).Scan(&exists); err != nil {
		writeErr(w, http.StatusInternalServerError, fmt.Sprintf("check flag: %v", err))
		return
	}
	if !exists {
		writeErr(w, http.StatusNotFound, "no flag named "+key)
		return
	}
	if err := flick.DeleteFlag(r.Context(), pool, key); err != nil {
		writeErr(w, http.StatusInternalServerError, fmt.Sprintf("delete flag: %v", err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "key": key})
}
