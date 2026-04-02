package metricq

import (
	"encoding/json"
	"testing"
)

type rpcTestRequest struct {
	RpcMessage
	Query string `json:"query"`
}

func TestMarshalRPCPayloadInjectsFunction(t *testing.T) {
	data, err := marshalRPCPayload(rpcTestRequest{
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
	if got := payload["query"]; got != "foo" {
		t.Fatalf("query = %v, want %q", got, "foo")
	}
}

func TestMarshalRPCPayloadAcceptsMatchingFunction(t *testing.T) {
	type request struct {
		RpcMessage
		Query string `json:"query"`
	}

	data, err := marshalRPCPayload(request{
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

func TestMarshalRPCPayloadRejectsEmptyFunction(t *testing.T) {
	_, err := marshalRPCPayload(rpcTestRequest{})
	if err == nil {
		t.Fatal("marshalRPCPayload succeeded unexpectedly")
	}
}

type rpcMismatchedRequest struct {
	Function string `json:"function"`
}

func (req rpcMismatchedRequest) RPCFunction() string {
	return "metrics.search"
}

func TestMarshalRPCPayloadRejectsMismatchedFunction(t *testing.T) {
	_, err := marshalRPCPayload(rpcMismatchedRequest{Function: "get_metrics"})
	if err == nil {
		t.Fatal("marshalRPCPayload succeeded unexpectedly")
	}
}

type rpcNonObjectRequest struct{}

func (req rpcNonObjectRequest) RPCFunction() string {
	return "metrics.search"
}

func (req rpcNonObjectRequest) MarshalJSON() ([]byte, error) {
	return json.Marshal([]string{"foo"})
}

func TestMarshalRPCPayloadRejectsNonObject(t *testing.T) {
	_, err := marshalRPCPayload(rpcNonObjectRequest{})
	if err == nil {
		t.Fatal("marshalRPCPayload succeeded unexpectedly")
	}
}
