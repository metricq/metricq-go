package metricq

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

func TestMarshalHistoryQueryContainsFields(t *testing.T) {
	start := time.Unix(0, 11)
	end := time.Unix(0, 22)
	raw, err := marshalHistoryQuery(start, end, 33, HistoryRequestTypeFlexTimeline)
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}

	decoded := &HistoryRequest{}
	if err := proto.Unmarshal(raw, decoded); err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}

	if decoded.GetStartTime() != 11 || decoded.GetEndTime() != 22 || decoded.GetIntervalMax() != 33 || decoded.GetType() != HistoryRequestTypeFlexTimeline {
		t.Fatalf("unexpected decoded values: %+v", decoded)
	}
}

func TestUnmarshalHistoryDataResponseValues(t *testing.T) {
	wire := &HistoryResponse{
		Metric:    "metric.values",
		TimeDelta: []int64{1_000_000, 2_000_000},
		Value:     []float64{1.5, 2.5},
	}
	payload, err := proto.Marshal(wire)
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}

	decoded, err := unmarshalHistoryDataResponse(payload)
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}
	if decoded.Metric != "metric.values" {
		t.Fatalf("metric mismatch: %s", decoded.Metric)
	}
	if len(decoded.TimeDelta) != 2 || len(decoded.Values) != 2 {
		t.Fatalf("unexpected lengths: %d %d", len(decoded.TimeDelta), len(decoded.Values))
	}
	if decoded.Values[0] != 1.5 || decoded.Values[1] != 2.5 {
		t.Fatalf("unexpected values: %#v", decoded.Values)
	}
}

func TestUnmarshalHistoryDataResponseAggregates(t *testing.T) {
	wire := &HistoryResponse{
		Metric:    "metric.agg",
		TimeDelta: []int64{1_000_000},
		Aggregate: []*HistoryResponse_Aggregate{{
			Minimum:    1,
			Maximum:    3,
			Sum:        8,
			Count:      4,
			Integral:   123,
			ActiveTime: 456,
		}},
	}
	payload, err := proto.Marshal(wire)
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}

	decoded, err := unmarshalHistoryDataResponse(payload)
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}
	if len(decoded.Agg) != 1 {
		t.Fatalf("unexpected agg len: %d", len(decoded.Agg))
	}
	if decoded.Agg[0].Count != 4 {
		t.Fatalf("unexpected count: %d", decoded.Agg[0].Count)
	}
	if decoded.Agg[0].Integral != 123 || decoded.Agg[0].ActiveTime != 456 {
		t.Fatalf("unexpected aggregate payload: %+v", decoded.Agg[0])
	}
}
