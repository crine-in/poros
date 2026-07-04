package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/crine/poros"
)

// Server wraps a poros.Cache and exposes REST HTTP endpoints.
type Server struct {
	cache  poros.Cache[string, any]
	logger *log.Logger
}

// NewServer creates a new HTTP cache server instance.
func NewServer(cache poros.Cache[string, any]) *Server {
	return &Server{
		cache:  cache,
		logger: log.Default(),
	}
}

// Handler returns the HTTP handler containing all registered routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/keys/", s.handleKeys)
	mux.HandleFunc("/stats", s.handleStats)
	mux.HandleFunc("/clear", s.handleClear)
	return s.loggingMiddleware(mux)
}

func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		s.logger.Printf("%s %s %s %s", r.Method, r.RequestURI, r.RemoteAddr, time.Since(start))
	})
}

func (s *Server) handleKeys(w http.ResponseWriter, r *http.Request) {
	// Path layout: /keys/{key} or /keys/{key}/increment or /keys/{key}/decrement
	pathSuffix := strings.TrimPrefix(r.URL.Path, "/keys/")
	if pathSuffix == "" {
		s.respondError(w, http.StatusBadRequest, "missing key in path")
		return
	}

	if strings.HasSuffix(pathSuffix, "/increment") {
		key := strings.TrimSuffix(pathSuffix, "/increment")
		if key == "" {
			s.respondError(w, http.StatusBadRequest, "missing key for increment")
			return
		}
		s.handleIncrement(w, r, key)
		return
	}

	if strings.HasSuffix(pathSuffix, "/decrement") {
		key := strings.TrimSuffix(pathSuffix, "/decrement")
		if key == "" {
			s.respondError(w, http.StatusBadRequest, "missing key for decrement")
			return
		}
		s.handleDecrement(w, r, key)
		return
	}

	s.handleKV(w, r, pathSuffix)
}

type setRequest struct {
	Value any    `json:"value"`
	TTL   string `json:"ttl"` // optional duration string e.g., "5m", "10s"
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request, key string) {
	switch r.Method {
	case http.MethodGet:
		val, ttl, ok := s.cache.GetWithTTL(key)
		if !ok {
			s.respondError(w, http.StatusNotFound, "key not found")
			return
		}

		s.respondJSON(w, http.StatusOK, map[string]any{
			"key":           key,
			"value":         val,
			"ttl_remaining": ttl.String(),
		})

	case http.MethodPost, http.MethodPut:
		var req setRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON payload: %v", err))
			return
		}

		var ttl time.Duration
		if req.TTL != "" {
			parsed, err := time.ParseDuration(req.TTL)
			if err != nil {
				s.respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid ttl duration format: %v", err))
				return
			}
			ttl = parsed
		}

		s.cache.Set(key, req.Value, ttl)
		s.respondJSON(w, http.StatusOK, map[string]string{
			"status":  "success",
			"message": "key set successfully",
		})

	case http.MethodDelete:
		deleted := s.cache.Delete(key)
		if !deleted {
			s.respondError(w, http.StatusNotFound, "key not found")
			return
		}
		s.respondJSON(w, http.StatusOK, map[string]string{
			"status":  "success",
			"message": "key deleted",
		})

	default:
		w.Header().Set("Allow", "GET, POST, PUT, DELETE")
		s.respondError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

type counterRequest struct {
	Delta int64 `json:"delta"`
}

func (s *Server) handleIncrement(w http.ResponseWriter, r *http.Request, key string) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		s.respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req counterRequest
	// Decode optional body (default to delta=1 if empty or not provided)
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Delta == 0 {
		req.Delta = 1
	}

	newVal, err := s.cache.Increment(key, req.Delta)
	if err != nil {
		s.respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.respondJSON(w, http.StatusOK, map[string]any{
		"key":   key,
		"value": newVal,
	})
}

func (s *Server) handleDecrement(w http.ResponseWriter, r *http.Request, key string) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		s.respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req counterRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Delta == 0 {
		req.Delta = 1
	}

	newVal, err := s.cache.Decrement(key, req.Delta)
	if err != nil {
		s.respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.respondJSON(w, http.StatusOK, map[string]any{
		"key":   key,
		"value": newVal,
	})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		s.respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	stats := s.cache.Stats()
	s.respondJSON(w, http.StatusOK, stats)
}

func (s *Server) handleClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		s.respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	s.cache.Clear()
	s.respondJSON(w, http.StatusOK, map[string]string{
		"status":  "success",
		"message": "cache cleared",
	})
}

func (s *Server) respondJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func (s *Server) respondError(w http.ResponseWriter, status int, message string) {
	s.respondJSON(w, status, map[string]string{
		"status":  "error",
		"message": message,
	})
}
