//go:build e2e

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/codetesla51/phylax"
	"github.com/jackc/pgx/v5/pgxpool"
)

const apiKey = "dashboard_api_test"

func newTestMux(t *testing.T) http.Handler {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), requireDB(t))
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(func() {
		cleanupFlag(t, pool, apiKey)
		pool.Close()
	})
	// zero-value Server works, but metrics come from a real provider only at
	// serve time — for API tests any Server is fine (nil → zero metrics).
	return newDashboardMux(pool, phylax.NewServer(nil, nil))
}

func req(t *testing.T, mux http.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body != nil {
		r = httptest.NewRequest(method, path, bytes.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	return w
}

func TestDashboardServesHTML(t *testing.T) {
	w := req(t, newTestMux(t), http.MethodGet, "/", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("GET / = %d, want 200", w.Code)
	}
	if !strings.Contains(w.Body.String(), "flick") || !strings.Contains(w.Body.String(), "api/flags") {
		t.Errorf("dashboard HTML missing expected markers")
	}
}

func TestDashboardAPICRUD(t *testing.T) {
	mux := newTestMux(t)

	// empty list
	w := req(t, mux, http.MethodGet, "/api/flags", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("GET list = %d, want 200", w.Code)
	}

	// create
	body, _ := json.Marshal(map[string]any{
		"key": apiKey, "state": "ENABLED", "defaultVariant": "on",
		"variants":  map[string]any{"on": true, "off": false},
		"targeting": map[string]any{}, "metadata": map[string]any{"owner": "api"},
	})
	w = req(t, mux, http.MethodPost, "/api/flags", body)
	if w.Code != http.StatusOK {
		t.Fatalf("POST create = %d, want 200: %s", w.Code, w.Body.String())
	}

	// appears in list
	w = req(t, mux, http.MethodGet, "/api/flags", nil)
	var list struct {
		Flags []map[string]any `json:"flags"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	found := false
	for _, f := range list.Flags {
		if f["key"] == apiKey && f["state"] == "ENABLED" {
			found = true
		}
	}
	if !found {
		t.Errorf("created flag not in list response")
	}

	// update (upsert) changes state
	body, _ = json.Marshal(map[string]any{
		"key": apiKey, "state": "DISABLED", "defaultVariant": "off",
		"variants":  map[string]any{"on": true, "off": false},
		"targeting": map[string]any{}, "metadata": map[string]any{},
	})
	if w = req(t, mux, http.MethodPost, "/api/flags", body); w.Code != http.StatusOK {
		t.Fatalf("POST update = %d, want 200", w.Code)
	}
	w = req(t, mux, http.MethodGet, "/api/flags", nil)
	_ = json.Unmarshal(w.Body.Bytes(), &list)
	for _, f := range list.Flags {
		if f["key"] == apiKey && f["state"] != "DISABLED" {
			t.Errorf("update did not persist: %v", f)
		}
	}

	// validation: bad state
	body, _ = json.Marshal(map[string]any{
		"key": apiKey, "state": "MAYBE", "defaultVariant": "on", "variants": map[string]any{"on": true},
	})
	if w = req(t, mux, http.MethodPost, "/api/flags", body); w.Code != http.StatusBadRequest {
		t.Errorf("bad state = %d, want 400", w.Code)
	}
	// validation: defaultVariant not in variants
	body, _ = json.Marshal(map[string]any{
		"key": apiKey, "state": "ENABLED", "defaultVariant": "zzz", "variants": map[string]any{"on": true},
	})
	if w = req(t, mux, http.MethodPost, "/api/flags", body); w.Code != http.StatusBadRequest {
		t.Errorf("bad defaultVariant = %d, want 400", w.Code)
	}

	// delete
	w = req(t, mux, http.MethodDelete, "/api/flags/"+apiKey, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("DELETE = %d, want 200", w.Code)
	}
	// gone from list and DB
	var n int
	if err := newTestDB(t).QueryRow(context.Background(), `SELECT count(*) FROM flags WHERE key=$1`, apiKey).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Errorf("flags rows = %d after delete, want 0", n)
	}
	// delete unknown → 404
	w = req(t, mux, http.MethodDelete, "/api/flags/"+apiKey, nil)
	if w.Code != http.StatusNotFound {
		t.Errorf("DELETE unknown = %d, want 404", w.Code)
	}
}

func newTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), requireDB(t))
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	return pool
}
