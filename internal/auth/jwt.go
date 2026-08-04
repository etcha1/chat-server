package auth

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

var (
	loginURL = "http://localhost:3000/login"
	httpClient = &http.Client{Timeout: 10 * time.Second}
)

func Login(username, password string) error {
	url := loginURL

	// 1. Initialize a client with a strict timeout to avoid leaking goroutines
	client := httpClient

	payload := strings.NewReader(`{"email":"` + username + `","password":"` + password + `"}`)

	// 2. Build the HTTP request
	req, err := http.NewRequest(http.MethodPost, url, payload)
	if err != nil {
		log.Printf("Error creating request: %v", err)
		return err
	}

	// 3. Set necessary headers
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	// 4. Send the request
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("API call failed: %v", err)
		return err
	}

	// 5. CRITICAL: Always defer closing the response body to prevent resource leaks
	defer resp.Body.Close()

	// 6. Check the HTTP status code
	if resp.StatusCode != http.StatusOK {
		log.Printf("Unexpected status code: %d", resp.StatusCode)
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}
