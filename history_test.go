package metricq

import (
	"net/url"
	"testing"
)

func TestDeriveHistoryDataURLWithVHost(t *testing.T) {
	baseURL, err := url.Parse("amqp://user:pass@rabbitmq:5672/")
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}

	got, err := deriveHistoryDataURL(baseURL, "vhost:/")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "amqp://user:pass@rabbitmq:5672/"
	if got.String() != want {
		t.Fatalf("unexpected url: got %q want %q", got.String(), want)
	}
}
