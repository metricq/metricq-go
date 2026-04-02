package metricq

import (
	"encoding/json"
	"testing"
)

func TestMarshalRPCPayloadInjectsFunction(t *testing.T) {
	data, err := marshalRPCPayload("metrics.search", map[string]any{
		"query": "foo",
	})
	if err != nil {
		t.Fatalf("marshalRPCPayload returned error: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	if got := payload["function"]; got != "metrics.search" {
		t.Fatalf("function = %v, want %q", got, "metrics.search")
	}
	if got := payload["query"]; got != "foo" {
		t.Fatalf("query = %v, want %q", got, "foo")
	}
}

func TestMarshalRPCPayloadAcceptsMatchingFunction(t *testing.T) {
	type request struct {
		RpcMessage
		Query string `json:"query"`
	}

	data, err := marshalRPCPayload("metrics.search", request{
		RpcMessage: RpcMessage{Function: "metrics.search"},
		Query:      "foo",
	})
	if err != nil {
		t.Fatalf("marshalRPCPayload returned error: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got := payload["function"]; got != "metrics.search" {
		t.Fatalf("function = %v, want %q", got, "metrics.search")
	}
}

func TestMarshalRPCPayloadRejectsMismatchedFunction(t *testing.T) {
	type request struct {
		RpcMessage
	}

	_, err := marshalRPCPayload("metrics.search", request{
		RpcMessage: RpcMessage{Function: "get_metrics"},
	})
	if err == nil {
		t.Fatal("marshalRPCPayload succeeded unexpectedly")
	}
}

func TestMarshalRPCPayloadRejectsNonObject(t *testing.T) {
	_, err := marshalRPCPayload("metrics.search", []string{"foo"})
	if err == nil {
		t.Fatal("marshalRPCPayload succeeded unexpectedly")
	}
}
