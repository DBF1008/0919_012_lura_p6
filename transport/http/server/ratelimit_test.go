// SPDX-License-Identifier: Apache-2.0

package server

import (
	"errors"
	"net/http/httptest"
	"testing"
)

type fakeRateLimitError struct{}

func (fakeRateLimitError) Error() string          { return "rate limit exceeded" }
func (fakeRateLimitError) StatusCode() int        { return 429 }
func (fakeRateLimitError) RetryAfterSeconds() int { return 5 }

func TestWriteRateLimitError(t *testing.T) {
	w := httptest.NewRecorder()
	if !WriteRateLimitError(w, fakeRateLimitError{}) {
		t.Fatal("the rate limited error must be recognized")
	}
	if w.Code != 429 {
		t.Fatalf("expected 429, got %d", w.Code)
	}
	if got := w.Header().Get("Retry-After"); got != "5" {
		t.Fatalf("expected Retry-After 5, got %q", got)
	}
}

func TestWriteRateLimitError_otherErrors(t *testing.T) {
	w := httptest.NewRecorder()
	if WriteRateLimitError(w, errors.New("boom")) {
		t.Fatal("unrelated errors must not be handled")
	}
	if w.Code != 200 {
		t.Fatalf("nothing should be written, got %d", w.Code)
	}
}
