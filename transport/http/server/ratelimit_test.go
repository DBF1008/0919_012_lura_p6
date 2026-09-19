// SPDX-License-Identifier: Apache-2.0

package server

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

func TestRateLimitHandler_SetsRetryAfter(t *testing.T) {
	handler := RateLimitHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("unexpected status code: %d", rec.Code)
	}
	if got := rec.Header().Get(RetryAfterHeaderName); got != strconv.Itoa(DefaultRetryAfter) {
		t.Errorf("unexpected %s header: %q", RetryAfterHeaderName, got)
	}
}

func TestRateLimitHandler_PreservesExistingRetryAfter(t *testing.T) {
	handler := RateLimitHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(RetryAfterHeaderName, "42")
		w.WriteHeader(http.StatusTooManyRequests)
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if got := rec.Header().Get(RetryAfterHeaderName); got != "42" {
		t.Errorf("the existing %s header should have been preserved, got %q", RetryAfterHeaderName, got)
	}
}

func TestRateLimitHandler_PassesThroughOtherStatuses(t *testing.T) {
	handler := RateLimitHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("unexpected status code: %d", rec.Code)
	}
	if got := rec.Header().Get(RetryAfterHeaderName); got != "" {
		t.Errorf("unexpected %s header: %q", RetryAfterHeaderName, got)
	}
	if rec.Body.String() != "ok" {
		t.Errorf("unexpected body: %q", rec.Body.String())
	}
}
