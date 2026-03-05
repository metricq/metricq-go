package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"time"

	metricq "github.com/metricq/metricq-go"
)

func main() {
	server := flag.String("server", "amqp://admin:admin@localhost", "MetricQ server URL")
	token := flag.String("token", "source-go-example", "A token to identify thiss client on the MetricQ network")
	metric := flag.String("metric", "dummy.go.source", "the metric to subscribe to")

	flag.Parse()

	run_sink(*token, *server, *metric)
}

func run_sink(token, server, metric string) {
	sink, err := metricq.NewSink(token, server)
	if err != nil {
		slog.Error("failed to create sink", "err", err)
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err = sink.Connect(ctx); err != nil {
		slog.Error("failed to connect", "err", err)
		panic(err)
	}
	defer sink.Close()

	slog.Info("Connected to MetricQ")

	runningCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	err = sink.Subscribe(ctx, runningCtx, []string{metric}, time.Hour)
	if err != nil {
		slog.Error("failed to subscribe", "metric", metric, "err", err)
		panic(err)
	}

	measurements := make(chan metricq.MetricDataPoint)
	if err = sink.NotifyDataPoint(measurements); err != nil {
		slog.Error("failed to set data point channel", "err", err)
		panic(err)
	}

loop:
	for {
		select {
		case datapoint, ok := <-measurements:
			if !ok {
				break loop
			}

			slog.Info("received datapoint", "metric", datapoint.Metric, "value", datapoint.Value, "timestamp", datapoint.Timestamp)
		case <-runningCtx.Done():
			break loop
		}
	}
}
