package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"math"
	"time"

	metricq "github.com/metricq/metricq-go"
)

type ExampleSourceConfig struct {
	Id  string `json:"_id"`
	Rev string `json:"_rev"`
}

type ExampleSourceMetricMetadata struct {
	metricq.MetricMetadata
	Zebras string `json:"zebras"`
}

func main() {
	server := flag.String("server", "amqp://admin:admin@localhost", "MetricQ server URL")
	token := flag.String("token", "source-go-example", "A token to identify thiss client on the MetricQ network")

	flag.Parse()

	run_source(*token, *server)
}

func run_source(token, server string) {
	src, err := metricq.NewSource(token, server)
	if err != nil {
		log.Fatalf("Failed to create source: %s", err)
	}

	defer src.Close()

	log.Print("Establishing Connection to MetricQ...")
	err = src.Connect(context.Background())
	if err != nil {
		log.Panicf("failed to connect to MetricQ: %v", err)
	}
	log.Print("Done.")

	go src.HandleDiscover(context.Background(), "1.0.0")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	resp, err := src.Register(ctx)
	if err != nil {
		log.Panicf("failed to register source: %v", err)
	}

	config := new(ExampleSourceConfig)
	if err := json.Unmarshal(resp, config); err != nil {
		log.Panicf("failed to parse Source config: %v", err)
	}

	log.Printf("Received config: %s", config)

	src.DeclareMetrics(ctx, map[string]interface{}{
		"go.dummy.source": ExampleSourceMetricMetadata{
			MetricMetadata: metricq.MetricMetadata{
				Description: "Dummy metric from the go example source",
				Rate:        0.1,
				Unit:        "puppies",
			},
			Zebras: "yes please!",
		},
	})

	metric := src.Metric("go.dummy.source")

	log.Print("Starting to send data points")

	for {
		tp := time.Now()
		if err := metric.Send(context.Background(), tp, math.Sin(2*math.Pi*float64(tp.UnixNano())/1e10)); err != nil {
			log.Panicf("failed to send: %v", err)
		}

		time.Sleep(100 * time.Millisecond)
	}

}
