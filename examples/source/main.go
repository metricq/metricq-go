package main

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"time"

	"github.com/alecthomas/kong"
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

var CLI struct {
	Server string `help:"MetricQ server URL." default:"amqp://admin:admin@localhost"`
	Token  string `help:"A token to identify this client on the MetricQ network." default:"source-go-example"`
}

func main() {
	cli := kong.Parse(&CLI)

	switch cli.Command() {
	case "":
		run_source(CLI.Server, CLI.Token)
	default:
		panic(cli.Command())
	}
}

func run_source(server, token string) {
	agent := metricq.NewAgent(token, server)

	defer agent.Close()

	log.Print("Establishing Connection to MetricQ...")
	agent.Connect()
	log.Print("Done.")

	go agent.HandleDiscover(context.Background(), "1.0.0")

	var src metricq.Source

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	resp := src.Register(ctx, agent)

	config := new(ExampleSourceConfig)
	err := json.Unmarshal(resp, config)
	if err != nil {
		log.Panicf("Failed to parse Source config: %s", err)
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

	log.Print("String to send data points")

	for {
		tp := time.Now().UnixNano()
		metric.Send(context.Background(), tp, math.Sin(2*math.Pi*float64(tp)/1e10))

		time.Sleep(100 * time.Millisecond)
	}

}
