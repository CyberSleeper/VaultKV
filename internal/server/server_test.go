package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"vault-kv/internal/store"
)

// BEGIN AI SECTION

// newTestServer stands up a real HTTP server backed by a real store, wired with
// the exact same mux that cmd/node/main.go builds. It returns the running
// httptest.Server; both the server and the underlying store are torn down when
// the test finishes.
func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	st, err := store.NewStore(t.TempDir(), "testnode")
	if err != nil {
		t.Fatalf("failed to init store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	srv := NewServer(st)
	mux := http.NewServeMux()
	mux.Handle("/set", LoggingMiddleware(http.HandlerFunc(srv.HandleSet)))
	mux.Handle("/get", LoggingMiddleware(http.HandlerFunc(srv.HandleGet)))

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts
}

// doSet POSTs a key/value to /set and returns the response.
func doSet(t *testing.T, ts *httptest.Server, key, value string) *http.Response {
	t.Helper()
	body, err := json.Marshal(KeyValueRequest{Key: key, Value: value})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	resp, err := http.Post(ts.URL+"/set", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /set failed: %v", err)
	}
	return resp
}

// doGet GETs a key from /get and returns the response.
func doGet(t *testing.T, ts *httptest.Server, key string) *http.Response {
	t.Helper()
	resp, err := http.Get(ts.URL + "/get?key=" + key)
	if err != nil {
		t.Fatalf("GET /get failed: %v", err)
	}
	return resp
}

func TestE2E_SetGetRoundTrip(t *testing.T) {
	ts := newTestServer(t)

	// SET should report 201 Created.
	setResp := doSet(t, ts, "user:1", "alice")
	defer setResp.Body.Close()
	if setResp.StatusCode != http.StatusCreated {
		t.Fatalf("SET: expected 201, got %d", setResp.StatusCode)
	}

	// GET should report 200 with the value we stored, as JSON.
	getResp := doGet(t, ts, "user:1")
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET: expected 200, got %d", getResp.StatusCode)
	}
	if ct := getResp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("GET: expected Content-Type application/json, got %q", ct)
	}

	var got Response
	if err := json.NewDecoder(getResp.Body).Decode(&got); err != nil {
		t.Fatalf("GET: failed to decode JSON body: %v", err)
	}
	if got.Value != "alice" {
		t.Errorf("GET: expected value 'alice', got %q", got.Value)
	}
	if got.Key != "user:1" {
		t.Errorf("GET: expected key 'user:1', got %q", got.Key)
	}
}

func TestE2E_GetMissingKeyReturns404(t *testing.T) {
	ts := newTestServer(t)

	resp := doGet(t, ts, "does_not_exist")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for missing key, got %d", resp.StatusCode)
	}
}

func TestE2E_OverwriteReturnsLatestValue(t *testing.T) {
	ts := newTestServer(t)

	r1 := doSet(t, ts, "counter", "1")
	r1.Body.Close()
	r2 := doSet(t, ts, "counter", "2")
	r2.Body.Close()

	resp := doGet(t, ts, "counter")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var got Response
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if got.Value != "2" {
		t.Errorf("expected latest value '2', got %q", got.Value)
	}
}

func TestE2E_SetWrongMethodReturns405(t *testing.T) {
	ts := newTestServer(t)

	// /set is a POST endpoint; a GET must be rejected.
	resp, err := http.Get(ts.URL + "/set")
	if err != nil {
		t.Fatalf("GET /set failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 for GET on /set, got %d", resp.StatusCode)
	}
}

func TestE2E_GetWrongMethodReturns405(t *testing.T) {
	ts := newTestServer(t)

	// /get is a GET endpoint; a POST must be rejected.
	resp, err := http.Post(ts.URL+"/get", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST /get failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 for POST on /get, got %d", resp.StatusCode)
	}
}

func TestE2E_SetMalformedJSONReturns400(t *testing.T) {
	ts := newTestServer(t)

	resp, err := http.Post(ts.URL+"/set", "application/json", strings.NewReader("{not valid json"))
	if err != nil {
		t.Fatalf("POST /set failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for malformed JSON, got %d", resp.StatusCode)
	}
}

func TestE2E_ConcurrentSetGet(t *testing.T) {
	ts := newTestServer(t)

	const workers = 25
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("k-%d", id)
			val := fmt.Sprintf("v-%d", id)

			setResp := doSet(t, ts, key, val)
			setResp.Body.Close()
			if setResp.StatusCode != http.StatusCreated {
				t.Errorf("worker %d: SET expected 201, got %d", id, setResp.StatusCode)
				return
			}

			getResp := doGet(t, ts, key)
			defer getResp.Body.Close()
			if getResp.StatusCode != http.StatusOK {
				t.Errorf("worker %d: GET expected 200, got %d", id, getResp.StatusCode)
				return
			}
			var got Response
			if err := json.NewDecoder(getResp.Body).Decode(&got); err != nil {
				t.Errorf("worker %d: decode failed: %v", id, err)
				return
			}
			if got.Value != val {
				t.Errorf("worker %d: expected %q, got %q", id, val, got.Value)
			}
		}(i)
	}
	wg.Wait()
}

// END AI SECTION
