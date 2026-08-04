package auth

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLoginReturnsNilOnSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != "/login" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	originalURL := loginURL
	originalClient := httpClient
	loginURL = server.URL + "/login"
	httpClient = server.Client()
	defer func() {
		loginURL = originalURL
		httpClient = originalClient
	}()

	if err := Login("demo@example.com", "secret"); err != nil {
		t.Fatalf("Login returned unexpected error: %v", err)
	}
}

func TestLoginReturnsErrorOnUnexpectedStatusCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()

	originalURL := loginURL
	originalClient := httpClient
	loginURL = server.URL + "/login"
	httpClient = server.Client()
	defer func() {
		loginURL = originalURL
		httpClient = originalClient
	}()

	err := Login("demo@example.com", "secret")
	if err == nil {
		t.Fatal("expected an error for unauthorized login")
	}
	if !strings.Contains(err.Error(), "unexpected status code") {
		t.Fatalf("expected unexpected status code error, got %v", err)
	}
}
