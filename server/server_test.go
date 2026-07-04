package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/crine-in/poros"
)

func setupTestServer() *Server {
	c := poros.New(poros.Config[string, any]{})
	return NewServer(c, "")
}

func TestServerKV(t *testing.T) {
	srv := setupTestServer()
	handler := srv.Handler()

	// 1. Get nonexistent key
	req := httptest.NewRequest(http.MethodGet, "/keys/missing", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}

	// 2. Set key
	payload := `{"value": "test_val", "ttl": "10s"}`
	req = httptest.NewRequest(http.MethodPost, "/keys/foo", bytes.NewBufferString(payload))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d, body: %s", w.Code, w.Body.String())
	}

	// 3. Get key
	req = httptest.NewRequest(http.MethodGet, "/keys/foo", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var getRes map[string]any
	if err := json.NewDecoder(w.Body).Decode(&getRes); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if getRes["value"] != "test_val" {
		t.Errorf("expected test_val, got %v", getRes["value"])
	}

	// 4. Delete key
	req = httptest.NewRequest(http.MethodDelete, "/keys/foo", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	// 5. Get deleted key
	req = httptest.NewRequest(http.MethodGet, "/keys/foo", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestServerCounters(t *testing.T) {
	srv := setupTestServer()
	handler := srv.Handler()

	// 1. Increment
	req := httptest.NewRequest(http.MethodPost, "/keys/page_views/increment", bytes.NewBufferString(`{"delta": 5}`))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d, body: %s", w.Code, w.Body.String())
	}

	var res map[string]any
	_ = json.NewDecoder(w.Body).Decode(&res)
	if res["value"] != float64(5) {
		t.Errorf("expected 5, got %v", res["value"])
	}

	// 2. Decrement
	req = httptest.NewRequest(http.MethodPost, "/keys/page_views/decrement", bytes.NewBufferString(`{"delta": 2}`))
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d, body: %s", w.Code, w.Body.String())
	}

	_ = json.NewDecoder(w.Body).Decode(&res)
	if res["value"] != float64(3) {
		t.Errorf("expected 3, got %v", res["value"])
	}
}

func TestServerStatsAndClear(t *testing.T) {
	srv := setupTestServer()
	handler := srv.Handler()

	// Set key to increase Stats.Sets
	payload := `{"value": "bar"}`
	req := httptest.NewRequest(http.MethodPost, "/keys/key1", bytes.NewBufferString(payload))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Fetch Stats
	req = httptest.NewRequest(http.MethodGet, "/stats", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var stats poros.Stats
	if err := json.NewDecoder(w.Body).Decode(&stats); err != nil {
		t.Fatalf("failed to decode stats: %v", err)
	}
	if stats.Sets != 1 {
		t.Errorf("expected sets count 1, got %d", stats.Sets)
	}

	// Clear cache
	req = httptest.NewRequest(http.MethodPost, "/clear", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	// Verify key1 is gone
	req = httptest.NewRequest(http.MethodGet, "/keys/key1", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestServerAuthorization(t *testing.T) {
	srv := NewServer(poros.New(poros.Config[string, any]{}), "my_test_secret_key")
	handler := srv.Handler()

	// 1. Request without header -> 401
	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", w.Code)
	}

	// 2. Request with invalid token -> 401
	req = httptest.NewRequest(http.MethodGet, "/stats", nil)
	req.Header.Set("Authorization", "Bearer wrong_token")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", w.Code)
	}

	// 3. Request with valid token -> 200
	req = httptest.NewRequest(http.MethodGet, "/stats", nil)
	req.Header.Set("Authorization", "Bearer my_test_secret_key")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}
