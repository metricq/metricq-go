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
	requestType := flag.String("request-type", "flex", "One of: flex, aggregate, last")
	intervalMax := flag.Duration("interval-max", 0, "Override interval_max directly (default computed from range/max-points)")
	timeout := flag.Duration("timeout", 10*time.Second, "Request timeout")
	flag.Parse()

	fromTS, toTS, err := parseRange(*from, *to)
	if err != nil {
		slog.Error("invalid time range", "err", err)
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

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch strings.ToLower(strings.TrimSpace(*requestType)) {
	case "flex", "flex-timeline":
		interval := resolveIntervalMax(fromTS, toTS, *intervalMax, *maxPoints)
		result, err := history.RequestTimeline(ctx, *metric, fromTS, toTS, interval)
		if err != nil {
			slog.Error("history query failed", "err", err)
			os.Exit(1)
		}
		printTimeline(result)

	case "aggregate", "agg":
		agg, dur, err := history.RequestAggregate(ctx, *metric, fromTS, toTS)
		if err != nil {
			slog.Error("history query failed", "err", err)
			os.Exit(1)
		}
		slog.Info("aggregate",
			"db_duration", dur,
			"min", agg.GetMinimum(),
			"max", agg.GetMaximum(),
			"count", agg.GetCount(),
		)

	case "last", "last-value":
		value, ts, dur, err := history.RequestLastValue(ctx, *metric)
		if err != nil {
			slog.Error("history query failed", "err", err)
			os.Exit(1)
		}
		slog.Info("last value", "db_duration", dur, "timestamp", ts, "value", value)

	default:
		slog.Error("unsupported request-type", "value", *requestType, "supported", "flex, aggregate, last")
		os.Exit(1)
	}
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

func resolveIntervalMax(from, to time.Time, explicit time.Duration, maxPoints int) time.Duration {
	if explicit > 0 {
		return explicit
	}
	if maxPoints <= 0 {
		maxPoints = 500
	}
	interval := (to.Sub(from) / time.Duration(maxPoints)) * 2
	if interval <= 0 {
		interval = time.Millisecond
	}
	return interval
}

func printTimeline(result metricq.TimelineResult) {
	switch r := result.(type) {
	case *metricq.TimelineValues:
		slog.Info("values timeline", "db_duration", r.Duration, "count", len(r.Values))
		if len(r.Values) > 0 {
			slog.Info("values", "first", r.Values[0], "last", r.Values[len(r.Values)-1])
		}
	case *metricq.TimelineAggregates:
		slog.Info("aggregate timeline", "db_duration", r.Duration, "count", len(r.Agg))
		if len(r.Agg) > 0 {
			first, last := r.Agg[0], r.Agg[len(r.Agg)-1]
			slog.Info("aggregates",
				"first_min", first.GetMinimum(),
				"first_max", first.GetMaximum(),
				"first_count", first.GetCount(),
				"last_min", last.GetMinimum(),
				"last_max", last.GetMaximum(),
				"last_count", last.GetCount(),
			)
		}
	}
}
