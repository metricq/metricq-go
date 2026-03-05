package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	metricq "github.com/metricq/metricq-go"
)

func main() {
	server := flag.String("server", "amqp://admin:admin@localhost", "MetricQ server URL")
	token := flag.String("token", "history-go-example", "Token to identify this client on the MetricQ network")
	metric := flag.String("metric", "dummy.go.source", "Metric to query")
	from := flag.String("from", "", "Start timestamp in RFC3339 (default: now-1h)")
	to := flag.String("to", "", "End timestamp in RFC3339 (default: now)")
	maxPoints := flag.Int("max-points", 500, "Target max points for flex timeline")
	requestType := flag.String("request-type", "flex", "One of: flex, aggregate, aggregate-timeline, last")
	intervalMax := flag.Duration("interval-max", 0, "Override interval_max directly (default computed from range/max-points)")
	timeout := flag.Duration("timeout", 10*time.Second, "Request timeout")
	flag.Parse()

	fromTS, toTS, err := parseRange(*from, *to)
	if err != nil {
		slog.Error("invalid time range", "err", err)
		os.Exit(1)
	}

	typeValue, err := parseRequestType(*requestType)
	if err != nil {
		slog.Error("invalid request-type", "err", err)
		os.Exit(1)
	}

	agent, err := metricq.NewAgent(*token, *server)
	if err != nil {
		slog.Error("failed to create agent", "err", err)
		os.Exit(1)
	}

	connectCtx, cancelConnect := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelConnect()
	if err := agent.Connect(connectCtx); err != nil {
		slog.Error("failed to connect", "err", err)
		os.Exit(1)
	}
	defer agent.Close()

	history, err := metricq.NewHistoryClient(context.Background(), agent)
	if err != nil {
		slog.Error("failed to create history client", "err", err)
		os.Exit(1)
	}
	defer history.Close()

	intervalNS := resolveIntervalMax(fromTS, toTS, *intervalMax, *maxPoints)

	query := metricq.HistoryQuery{
		Metric:      *metric,
		StartTimeNS: fromTS.UnixNano(),
		EndTimeNS:   toTS.UnixNano(),
		IntervalMax: intervalNS,
		RequestType: typeValue,
		Timeout:     *timeout,
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	resp, duration, err := history.Request(ctx, query)
	if err != nil {
		slog.Error("history query failed", "err", err)
		os.Exit(1)
	}
	if resp.Error != "" {
		slog.Warn("history returned error", "error", resp.Error)
	}

	printSummary(resp, duration)
}

func parseRange(fromRaw, toRaw string) (time.Time, time.Time, error) {
	now := time.Now()
	toTS := now
	fromTS := now.Add(-time.Hour)

	if strings.TrimSpace(fromRaw) != "" {
		parsed, err := time.Parse(time.RFC3339, fromRaw)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("parse from: %w", err)
		}
		fromTS = parsed
	}
	if strings.TrimSpace(toRaw) != "" {
		parsed, err := time.Parse(time.RFC3339, toRaw)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("parse to: %w", err)
		}
		toTS = parsed
	}
	if !toTS.After(fromTS) {
		return time.Time{}, time.Time{}, fmt.Errorf("to must be after from")
	}
	return fromTS, toTS, nil
}

func parseRequestType(value string) (metricq.HistoryRequestType, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "flex", "flex-timeline":
		return metricq.HistoryRequestTypeFlexTimeline, nil
	case "aggregate", "agg":
		return metricq.HistoryRequestTypeAggregate, nil
	case "aggregate-timeline", "agg-timeline":
		return metricq.HistoryRequestTypeAggregateTimeline, nil
	case "last", "last-value":
		return metricq.HistoryRequestTypeLastValue, nil
	default:
		return 0, fmt.Errorf("unsupported request type %q", value)
	}
}

func resolveIntervalMax(from, to time.Time, explicit time.Duration, maxPoints int) int64 {
	if explicit > 0 {
		return explicit.Nanoseconds()
	}
	if maxPoints <= 0 {
		maxPoints = 500
	}
	span := to.Sub(from)
	interval := (span / time.Duration(maxPoints)) * 2
	if interval <= 0 {
		interval = time.Millisecond
	}
	return interval.Nanoseconds()
}

func printSummary(resp metricq.HistoryDataResponse, duration float64) {
	slog.Info("history response",
		"metric", resp.Metric,
		"db_duration_seconds", duration,
		"time_delta_count", len(resp.TimeDelta),
		"value_count", len(resp.Values),
		"aggregate_count", len(resp.Agg),
	)

	if len(resp.Values) > 0 {
		first := resp.Values[0]
		last := resp.Values[len(resp.Values)-1]
		slog.Info("values", "first", first, "last", last)
	}
	if len(resp.Agg) > 0 {
		first := resp.Agg[0]
		last := resp.Agg[len(resp.Agg)-1]
		slog.Info("aggregates",
			"first_min", first.Minimum,
			"first_max", first.Maximum,
			"first_count", first.Count,
			"last_min", last.Minimum,
			"last_max", last.Maximum,
			"last_count", last.Count,
		)
	}
}
