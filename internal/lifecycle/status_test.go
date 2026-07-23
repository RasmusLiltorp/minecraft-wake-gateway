package lifecycle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestStatusRequiresExactBearerTokenAndDoesNotLeakIt(t *testing.T) {
	target := Target{Namespace: "minecraft", Name: "survival"}
	sessions := NewSessionStore(time.Now)
	coordinator := testCoordinator(&fakeWaker{}, &fakeReadiness{results: make(chan error)})
	t.Cleanup(coordinator.Close)
	handler := NewStatusHandler("secret-token", target, sessions, coordinator, func() bool { return true })

	for _, authorization := range []string{"", "Bearer wrong", "secret-token"} {
		req := httptest.NewRequest(http.MethodGet, "/v1/targets/minecraft/survival", nil)
		req.Header.Set("Authorization", authorization)
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		if res.Code != http.StatusUnauthorized {
			t.Fatalf("authorization %q returned %d, want 401", authorization, res.Code)
		}
		if body := res.Body.String(); contains(body, "secret-token") {
			t.Fatal("response leaked bearer token")
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/targets/minecraft/survival", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("status returned %d: %s", res.Code, res.Body.String())
	}
	var got TargetStatus
	if err := json.Unmarshal(res.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.APIVersion != StatusAPIVersion || got.Target != "minecraft/survival" {
		t.Fatalf("unexpected status: %#v", got)
	}
}

func TestHealthAndReadinessNeverRequireAuth(t *testing.T) {
	target := Target{Namespace: "minecraft", Name: "survival"}
	sessions := NewSessionStore(time.Now)
	coordinator := testCoordinator(&fakeWaker{}, &fakeReadiness{results: make(chan error)})
	t.Cleanup(coordinator.Close)
	ready := false
	handler := NewStatusHandler("secret-token", target, sessions, coordinator, func() bool { return ready })

	for _, path := range []string{"/healthz", "/readyz"} {
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, httptest.NewRequest(http.MethodGet, path, nil))
		want := http.StatusOK
		if path == "/readyz" {
			want = http.StatusServiceUnavailable
		}
		if res.Code != want {
			t.Fatalf("%s returned %d, want %d", path, res.Code, want)
		}
	}
	ready = true
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if res.Code != http.StatusOK {
		t.Fatalf("ready endpoint returned %d", res.Code)
	}
}

func contains(value, substring string) bool {
	for i := 0; i+len(substring) <= len(value); i++ {
		if value[i:i+len(substring)] == substring {
			return true
		}
	}
	return false
}
