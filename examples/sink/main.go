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
	metric := flag.String("metric", "dummy.go.source", "the metric to subscribe to")

	flag.Parse()

	run_sink(*token, *server, *metric)
}

func run_sink(token, server, metric string) {
	sink, err := metricq.NewSink(token, server)
	if err != nil {
		log.Panicf("falied to create sink: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err = sink.Connect(ctx); err != nil {
		log.Panicf("failed to connect: %v", err)
	}
	defer sink.Close()

	log.Print("Connected to MetricQ")

	err = sink.Subscribe(ctx, []string{metric}, time.Hour)
	if err != nil {
		log.Panicf("failed to subscribe (%s): %v", metric, err)
	}

}
