package metricq

import (
	"fmt"
	"iter"
	"math"
	"time"
)

// Mean returns the time-weighted mean value of the aggregate.
// Returns an error if ActiveTime is zero.
func (agg *HistoryResponse_Aggregate) Mean() (float64, error) {
	if agg.GetActiveTime() == 0 {
		return math.NaN(), fmt.Errorf("aggregate has zero active time")
	}
	return agg.GetIntegral() / float64(agg.GetActiveTime()), nil
}

// MeanSum returns the arithmetic mean of the aggregate (sum / count).
// Returns an error if Count is zero.
func (agg *HistoryResponse_Aggregate) MeanSum() (float64, error) {
	if agg.GetCount() == 0 {
		return math.NaN(), fmt.Errorf("aggregate has zero values")
	}
	return agg.GetSum() / float64(agg.GetCount()), nil
}

// All returns an iterator over (time, value) pairs.
func (r *TimelineValues) All() iter.Seq2[time.Time, float64] {
	return func(yield func(time.Time, float64) bool) {
		for i, t := range r.Times {
			if !yield(t, r.Values[i]) {
				return
			}
		}
	}
}

// All returns an iterator over (time, aggregate) pairs.
func (r *TimelineAggregates) All() iter.Seq2[time.Time, *HistoryResponse_Aggregate] {
	return func(yield func(time.Time, *HistoryResponse_Aggregate) bool) {
		for i, t := range r.Times {
			if !yield(t, r.Agg[i]) {
				return
			}
		}
	}
}
