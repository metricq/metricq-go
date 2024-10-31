package main

import (
	"context"
	"flag"
	"log"
	"time"

	metricq "github.com/metricq/metricq-go"
)

func main() {
	server := flag.String("server", "amqp://admin:admin@localhost", "MetricQ server URL")
	token := flag.String("token", "source-go-example", "A token to identify thiss client on the MetricQ network")
	metric := flag.String("metric", "dummy.go.source")

	flag.Parse()

	run_sink(*token, *server, *metric)
}

func run_sink(token, server, metric string) {
	sink := metricq.NewSink(token, server)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

	err := sink.Connect(ctx)
    if err != nil {
        log.Panicf("failed to connect: %v", err)
    }
    defer sink.Close()

    log.Print("Connected to MetricQ")

    err = sink.Subscribe(ctx, {metric}, time.Hour)
    if err != nil {
        log.Panicf("failed to subscribe (%s): %v", metric, err)
    }

}
