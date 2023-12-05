package main

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"time"

	metrigo "github.com/metricq/metrigo"
)

type ExampleSourceConfig struct {
	Id  string `json:"_id"`
	Rev string `json:"_rev"`
}

func main() {
	agent := metrigo.NewAgent("source-go-example", "amqp://admin:admin@localhost")

	defer agent.Close()

	log.Print("Establishing Connection to MetricQ...")
	agent.Connect()
	log.Print("Done.")

	var src metrigo.Source

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	resp := src.Register(ctx, agent)

	config := new(ExampleSourceConfig)
	err := json.Unmarshal(resp, config)
	if err != nil {
		log.Panicf("Failed to parse Source config: %s", err)
	}

	log.Printf("Received config: %s", config)

	src.DeclareMetrics(ctx, []string{"go.dummy.source"})

	metric := src.Metric("go.dummy.source")

    log.Print("String to send data points")

	for {
		tp := time.Now().UnixNano()
		metric.Send(context.Background(), tp, math.Sin(2*math.Pi*float64(tp)/1e10))

		time.Sleep(100 * time.Millisecond)
	}
}
