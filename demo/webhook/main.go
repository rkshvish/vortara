// Demo webhook server for the Vortara DLQ/replay demo.
//
// Behaviour:
//   - Returns HTTP 200 for all requests by default.
//   - Returns HTTP 500 for any entity key listed in the FAIL_KEYS env var
//     (comma-separated). This simulates a transient destination failure.
//   - Logs every received payload to stdout.
//   - GET /reset  — clears all received payloads (for scripting).
//   - GET /log    — returns received payload log as JSON.
//
// Usage:
//
//	FAIL_KEYS=lead_002 go run ./demo/webhook
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type received struct {
	At        time.Time      `json:"at"`
	EntityKey string         `json:"entity_key"`
	Payload   map[string]any `json:"payload"`
}

var (
	mu      sync.Mutex
	log_    []received
	failSet map[string]bool
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "18081"
	}

	failSet = make(map[string]bool)
	for _, k := range strings.Split(os.Getenv("FAIL_KEYS"), ",") {
		k = strings.TrimSpace(k)
		if k != "" {
			failSet[k] = true
			log.Printf("will return 500 for entity key: %q", k)
		}
	}

	http.HandleFunc("/webhook", handleWebhook)
	http.HandleFunc("/log", handleLog)
	http.HandleFunc("/reset", handleReset)

	log.Printf("demo webhook listening on :%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}

func handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var payload map[string]any
	_ = json.Unmarshal(body, &payload)

	entityKey := ""
	for _, field := range []string{"id", "entityKey", "entity_key"} {
		if v, ok := payload[field]; ok {
			entityKey = fmt.Sprintf("%v", v)
			break
		}
	}

	rec := received{At: time.Now().UTC(), EntityKey: entityKey, Payload: payload}
	mu.Lock()
	log_ = append(log_, rec)
	mu.Unlock()

	if failSet[entityKey] {
		log.Printf("500  %-20s (in FAIL_KEYS)", entityKey)
		http.Error(w, "simulated failure", http.StatusInternalServerError)
		return
	}

	log.Printf("200  %-20s %s", entityKey, body[:min(len(body), 120)])
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, `{"ok":true}`)
}

func handleLog(w http.ResponseWriter, _ *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(log_)
}

func handleReset(w http.ResponseWriter, _ *http.Request) {
	mu.Lock()
	log_ = nil
	mu.Unlock()
	fmt.Fprintln(w, "reset")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
