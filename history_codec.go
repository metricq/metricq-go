package metricq

import (
	"fmt"
	"math"

	"google.golang.org/protobuf/encoding/protowire"
)

func encodeHistoryRequest(req HistoryRequest) []byte {
	out := make([]byte, 0, 64)
	out = protowire.AppendTag(out, 1, protowire.VarintType)
	out = protowire.AppendVarint(out, uint64(req.StartTimeNS))
	out = protowire.AppendTag(out, 2, protowire.VarintType)
	out = protowire.AppendVarint(out, uint64(req.EndTimeNS))
	out = protowire.AppendTag(out, 3, protowire.VarintType)
	out = protowire.AppendVarint(out, uint64(req.IntervalMax))
	out = protowire.AppendTag(out, 4, protowire.VarintType)
	out = protowire.AppendVarint(out, uint64(req.RequestType))
	return out
}

func decodeHistoryResponse(payload []byte) (HistoryResponse, error) {
	var out HistoryResponse
	for len(payload) > 0 {
		num, typ, n := protowire.ConsumeTag(payload)
		if n < 0 {
			return out, fmt.Errorf("invalid history response tag")
		}
		payload = payload[n:]
		switch num {
		case 1:
			v, n := protowire.ConsumeString(payload)
			if n < 0 {
				return out, fmt.Errorf("invalid metric field")
			}
			out.Metric = v
			payload = payload[n:]
		case 2:
			values, consumed, err := consumeInt64Repeated(payload, typ)
			if err != nil {
				return out, err
			}
			out.TimeDelta = append(out.TimeDelta, values...)
			payload = payload[consumed:]
		case 6:
			blob, n := protowire.ConsumeBytes(payload)
			if n < 0 {
				return out, fmt.Errorf("invalid aggregate message")
			}
			agg, err := decodeAggregate(blob)
			if err != nil {
				return out, err
			}
			out.Agg = append(out.Agg, agg)
			payload = payload[n:]
		case 7:
			values, consumed, err := consumeDoubleRepeated(payload, typ)
			if err != nil {
				return out, err
			}
			out.Values = append(out.Values, values...)
			payload = payload[consumed:]
		case 8:
			v, n := protowire.ConsumeString(payload)
			if n < 0 {
				return out, fmt.Errorf("invalid error field")
			}
			out.Error = v
			payload = payload[n:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, payload)
			if n < 0 {
				return out, fmt.Errorf("invalid unknown field")
			}
			payload = payload[n:]
		}
	}
	return out, nil
}

func decodeAggregate(payload []byte) (HistoryAggregate, error) {
	var out HistoryAggregate
	for len(payload) > 0 {
		num, typ, n := protowire.ConsumeTag(payload)
		if n < 0 {
			return out, fmt.Errorf("invalid aggregate tag")
		}
		payload = payload[n:]
		switch num {
		case 1:
			v, n := protowire.ConsumeFixed64(payload)
			if n < 0 {
				return out, fmt.Errorf("invalid aggregate.minimum")
			}
			out.Minimum = math.Float64frombits(v)
			payload = payload[n:]
		case 2:
			v, n := protowire.ConsumeFixed64(payload)
			if n < 0 {
				return out, fmt.Errorf("invalid aggregate.maximum")
			}
			out.Maximum = math.Float64frombits(v)
			payload = payload[n:]
		case 3:
			v, n := protowire.ConsumeFixed64(payload)
			if n < 0 {
				return out, fmt.Errorf("invalid aggregate.sum")
			}
			out.Sum = math.Float64frombits(v)
			payload = payload[n:]
		case 4:
			v, n := protowire.ConsumeVarint(payload)
			if n < 0 {
				return out, fmt.Errorf("invalid aggregate.count")
			}
			out.Count = v
			payload = payload[n:]
		case 5:
			v, n := protowire.ConsumeFixed64(payload)
			if n < 0 {
				return out, fmt.Errorf("invalid aggregate.integral")
			}
			out.Integral = math.Float64frombits(v)
			payload = payload[n:]
		case 6:
			v, n := protowire.ConsumeVarint(payload)
			if n < 0 {
				return out, fmt.Errorf("invalid aggregate.active_time")
			}
			out.ActiveTime = int64(v)
			payload = payload[n:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, payload)
			if n < 0 {
				return out, fmt.Errorf("invalid aggregate unknown field")
			}
			payload = payload[n:]
		}
	}
	return out, nil
}

func consumeInt64Repeated(payload []byte, typ protowire.Type) ([]int64, int, error) {
	switch typ {
	case protowire.VarintType:
		v, n := protowire.ConsumeVarint(payload)
		if n < 0 {
			return nil, 0, fmt.Errorf("invalid int64 value")
		}
		return []int64{int64(v)}, n, nil
	case protowire.BytesType:
		blob, n := protowire.ConsumeBytes(payload)
		if n < 0 {
			return nil, 0, fmt.Errorf("invalid packed int64 values")
		}
		vals := make([]int64, 0, 64)
		for len(blob) > 0 {
			v, m := protowire.ConsumeVarint(blob)
			if m < 0 {
				return nil, 0, fmt.Errorf("invalid packed int64 element")
			}
			vals = append(vals, int64(v))
			blob = blob[m:]
		}
		return vals, n, nil
	default:
		return nil, 0, fmt.Errorf("unexpected wire type for int64 list")
	}
}

func consumeDoubleRepeated(payload []byte, typ protowire.Type) ([]float64, int, error) {
	switch typ {
	case protowire.Fixed64Type:
		v, n := protowire.ConsumeFixed64(payload)
		if n < 0 {
			return nil, 0, fmt.Errorf("invalid double value")
		}
		return []float64{math.Float64frombits(v)}, n, nil
	case protowire.BytesType:
		blob, n := protowire.ConsumeBytes(payload)
		if n < 0 {
			return nil, 0, fmt.Errorf("invalid packed double values")
		}
		vals := make([]float64, 0, 64)
		for len(blob) > 0 {
			v, m := protowire.ConsumeFixed64(blob)
			if m < 0 {
				return nil, 0, fmt.Errorf("invalid packed double element")
			}
			vals = append(vals, math.Float64frombits(v))
			blob = blob[m:]
		}
		return vals, n, nil
	default:
		return nil, 0, fmt.Errorf("unexpected wire type for double list")
	}
}
