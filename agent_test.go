package metricq

import (
	"encoding/json"
	"testing"
)

func TestRPCEnvelopeInjectsFunction(t *testing.T) {
	data, err := json.Marshal(rpcEnvelope{
		Function: "metrics.search",
		Payload: map[string]any{
			"query": "foo",
		},
	})
	if err != nil {
		t.Fatalf("json.Marshal returned error: %v", err)
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

func TestRPCEnvelopeOverridesPayloadFunction(t *testing.T) {
	data, err := json.Marshal(rpcEnvelope{
		Function: "metrics.search",
		Payload:  RpcMessage{Function: "get_metrics"},
	})
	if err != nil {
		t.Fatalf("json.Marshal returned error: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got := payload["function"]; got != "metrics.search" {
		t.Fatalf("function = %v, want %q", got, "metrics.search")
	}
}

func TestRPCEnvelopeAllowsNilPayload(t *testing.T) {
	data, err := json.Marshal(rpcEnvelope{Function: "discover"})
	if err != nil {
		t.Fatalf("json.Marshal returned error: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if len(payload) != 1 || payload["function"] != "discover" {
		t.Fatalf("unexpected payload: %#v", payload)
	}
}

type nonObjectPayload struct{}

func (nonObjectPayload) MarshalJSON() ([]byte, error) {
	return json.Marshal([]string{"foo"})
}

func TestRPCEnvelopeRejectsNonObjectPayload(t *testing.T) {
	_, err := json.Marshal(rpcEnvelope{
		Function: "metrics.search",
		Payload:  nonObjectPayload{},
	})
	if err == nil {
		t.Fatal("json.Marshal succeeded unexpectedly")
	}
}
