package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	metrigo "github.com/metricq/metrigo"
)

type ExampleSourceConfig struct {
    Id string `json:"_id"`
    Rev string `json:"_rev"`
}


func main() {
    agent := metrigo.New("source-go-example", "amqp://admin:admin@localhost")

	defer agent.Close()

	log.Print("Establishing Connection to MetricQ...")
	agent.Connect()
	log.Print("Done.")

    var src metrigo.Source

    ctx, cancel := context.WithTimeout(context.Background(), 60 * time.Second) 
    resp := src.Register(ctx, agent)

    config := new(ExampleSourceConfig)
    err := json.Unmarshal(resp, config)
    if err != nil {
        log.Panicf("Failed to parse Source config: %s", err)
    }

    log.Printf("Received config: %s", config)

    cancel()
}
