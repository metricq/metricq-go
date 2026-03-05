package metricq

import (
	"math"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

func TestEncodeHistoryRequestContainsFields(t *testing.T) {
	raw := encodeHistoryRequest(HistoryRequest{
		StartTimeNS: 11,
		EndTimeNS:   22,
		IntervalMax: 33,
		RequestType: HistoryRequestTypeFlexTimeline,
	})

	values := map[protowire.Number]uint64{}
	for len(raw) > 0 {
		num, typ, n := protowire.ConsumeTag(raw)
		if n < 0 {
			t.Fatalf("invalid tag")
		}
		raw = raw[n:]
		if typ != protowire.VarintType {
			t.Fatalf("unexpected type %v", typ)
		}
		v, m := protowire.ConsumeVarint(raw)
		if m < 0 {
			t.Fatalf("invalid varint")
		}
		values[num] = v
		raw = raw[m:]
	}

	if values[1] != 11 || values[2] != 22 || values[3] != 33 || values[4] != uint64(HistoryRequestTypeFlexTimeline) {
		t.Fatalf("unexpected encoded values: %#v", values)
	}
}

func TestDecodeHistoryResponseValues(t *testing.T) {
	payload := make([]byte, 0)
	payload = protowire.AppendTag(payload, 1, protowire.BytesType)
	payload = protowire.AppendString(payload, "metric.values")

	td := []byte{}
	td = protowire.AppendVarint(td, 1_000_000)
	td = protowire.AppendVarint(td, 2_000_000)
	payload = protowire.AppendTag(payload, 2, protowire.BytesType)
	payload = protowire.AppendBytes(payload, td)

	vals := []byte{}
	vals = protowire.AppendFixed64(vals, math.Float64bits(1.5))
	vals = protowire.AppendFixed64(vals, math.Float64bits(2.5))
	payload = protowire.AppendTag(payload, 7, protowire.BytesType)
	payload = protowire.AppendBytes(payload, vals)

	decoded, err := decodeHistoryResponse(payload)
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

func TestDecodeHistoryResponseAggregates(t *testing.T) {
	payload := make([]byte, 0)
	payload = protowire.AppendTag(payload, 1, protowire.BytesType)
	payload = protowire.AppendString(payload, "metric.agg")

	td := []byte{}
	td = protowire.AppendVarint(td, 1_000_000)
	payload = protowire.AppendTag(payload, 2, protowire.BytesType)
	payload = protowire.AppendBytes(payload, td)

	agg := []byte{}
	agg = protowire.AppendTag(agg, 1, protowire.Fixed64Type)
	agg = protowire.AppendFixed64(agg, math.Float64bits(1))
	agg = protowire.AppendTag(agg, 2, protowire.Fixed64Type)
	agg = protowire.AppendFixed64(agg, math.Float64bits(3))
	agg = protowire.AppendTag(agg, 3, protowire.Fixed64Type)
	agg = protowire.AppendFixed64(agg, math.Float64bits(8))
	agg = protowire.AppendTag(agg, 4, protowire.VarintType)
	agg = protowire.AppendVarint(agg, 4)
	agg = protowire.AppendTag(agg, 5, protowire.Fixed64Type)
	agg = protowire.AppendFixed64(agg, math.Float64bits(123))
	agg = protowire.AppendTag(agg, 6, protowire.VarintType)
	agg = protowire.AppendVarint(agg, 456)

	payload = protowire.AppendTag(payload, 6, protowire.BytesType)
	payload = protowire.AppendBytes(payload, agg)

	decoded, err := decodeHistoryResponse(payload)
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
