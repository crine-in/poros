package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/crine-in/poros"
	"github.com/crine-in/poros/server"
)

// loadEnv reads the local .env file (if present) and populates the process environment.
func loadEnv() {
	file, err := os.Open(".env")
	if err != nil {
		return // Silently skip if no .env file exists
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			val := strings.TrimSpace(parts[1])
			// Trim surrounding quotes
			val = strings.Trim(val, `"'`)
			os.Setenv(key, val)
		}
	}
}

// parseSize parses human-readable byte sizes e.g. "500KB", "10MB", "2GB" into raw bytes.
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" || s == "0" {
		return 0, nil
	}
	var multiplier int64 = 1
	if strings.HasSuffix(s, "KB") {
		multiplier = 1024
		s = strings.TrimSuffix(s, "KB")
	} else if strings.HasSuffix(s, "MB") {
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "MB")
	} else if strings.HasSuffix(s, "GB") {
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "GB")
	} else if strings.HasSuffix(s, "B") {
		s = strings.TrimSuffix(s, "B")
	}
	var val int64
	if _, err := fmt.Sscanf(s, "%d", &val); err != nil {
		return 0, err
	}
	return val * multiplier, nil
}

func main() {
	// Load environment variables from .env
	loadEnv()

	// 1. Command-line flags configuration
	port := flag.Int("port", 8080, "TCP port to listen on")
	shards := flag.Int("shards", 0, "Number of cache shards (rounded up to nearest power of 2)")
	capacity := flag.Int("capacity", 0, "Maximum cache capacity (0 = unlimited)")
	maxItemSizeStr := flag.String("max-item-size", "0", "Maximum size of an individual item (e.g. 500KB, 10MB)")
	maxMemoryStr := flag.String("max-memory", "0", "Maximum total memory cache is allowed to use (e.g. 100MB, 2GB)")
	policyVal := flag.Int("policy", 3, "Eviction policy (0=LRU, 1=LFU, 2=FIFO, 3=None)")
	defaultTTLStr := flag.String("ttl", "0s", "Default Time-To-Live duration (e.g. 5m, 1h, 0s = disabled)")
	defaultTTIStr := flag.String("tti", "0s", "Default Time-To-Idle duration (e.g. 2m, 0s = disabled)")
	janitorStr := flag.String("janitor", "1m", "Background sweep cleaner interval")

	flag.Parse()

	// Allow environment variables to override if command line remains at default
	if envPort := os.Getenv("PORT"); envPort != "" && *port == 8080 {
		var p int
		if _, err := fmt.Sscanf(envPort, "%d", &p); err == nil {
			*port = p
		}
	}
	if envMaxItemSize := os.Getenv("MAX_ITEM_SIZE"); envMaxItemSize != "" && *maxItemSizeStr == "0" {
		*maxItemSizeStr = envMaxItemSize
	}
	if envMaxMemory := os.Getenv("MAX_MEMORY"); envMaxMemory != "" && *maxMemoryStr == "0" {
		*maxMemoryStr = envMaxMemory
	}

	// 2. Parse sizes and durations
	maxItemSize, err := parseSize(*maxItemSizeStr)
	if err != nil {
		log.Fatalf("invalid max-item-size: %v", err)
	}

	maxMemory, err := parseSize(*maxMemoryStr)
	if err != nil {
		log.Fatalf("invalid max-memory: %v", err)
	}

	defaultTTL, err := time.ParseDuration(*defaultTTLStr)
	if err != nil {
		log.Fatalf("invalid default TTL duration: %v", err)
	}

	defaultTTI, err := time.ParseDuration(*defaultTTIStr)
	if err != nil {
		log.Fatalf("invalid default TTI duration: %v", err)
	}

	janitorInterval, err := time.ParseDuration(*janitorStr)
	if err != nil {
		log.Fatalf("invalid janitor sweep interval: %v", err)
	}

	// 3. Map eviction policy
	var policy poros.EvictionType
	switch *policyVal {
	case 0:
		policy = poros.EvictionLRU
	case 1:
		policy = poros.EvictionLFU
	case 2:
		policy = poros.EvictionFIFO
	default:
		policy = poros.EvictionNone
	}

	log.Printf("Starting Poros cache daemon...")
	log.Printf("Config -> Shards: %d, Capacity: %d, MaxItemSize: %d bytes, MaxMemory: %d bytes, Policy: %d, DefaultTTL: %v, DefaultTTI: %v, Janitor: %v",
		*shards, *capacity, maxItemSize, maxMemory, policy, defaultTTL, defaultTTI, janitorInterval)

	authToken := os.Getenv("POROS_KEY")
	if authToken != "" {
		log.Printf("Security Layer: Enabled (Bearer Token authentication active)")
	} else {
		log.Printf("Security Layer: Disabled (Warning: endpoints are unsecured)")
	}

	// 4. Initialize cache core
	cache := poros.New(poros.Config[string, any]{
		Shards:          *shards,
		Capacity:        *capacity,
		MaxItemSize:     maxItemSize,
		MaxMemory:       maxMemory,
		EvictionPolicy:  policy,
		DefaultTTL:      defaultTTL,
		DefaultTTI:      defaultTTI,
		JanitorInterval: janitorInterval,
	})

	// 5. Initialize HTTP server
	apiServer := server.NewServer(cache, authToken)
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: apiServer.Handler(),
	}

	// 6. Start server in background
	go func() {
		log.Printf("Poros HTTP cache service listening on :%d", *port)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server failed to listen: %v", err)
		}
	}()

	// 7. Setup graceful shutdown listener
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	// Block until signal is received
	sig := <-stopChan
	log.Printf("Received signal %v. Initiating graceful shutdown...", sig)

	// Shutdown context (30 seconds timeout limit)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	} else {
		log.Printf("HTTP listener stopped successfully.")
	}

	// Close cache resources (stops active background clean sweep routine)
	if err := cache.Close(); err != nil {
		log.Printf("Cache close error: %v", err)
	} else {
		log.Printf("Cache background cleaner halted successfully.")
	}

	log.Printf("Poros daemon terminated cleanly.")
}
