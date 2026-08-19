# metricq-go

[![Go](https://github.com/metricq/metricq-go/actions/workflows/go.yml/badge.svg)](https://github.com/metricq/metricq-go/actions/workflows/go.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/metricq/metricq-go.svg)](https://pkg.go.dev/github.com/metricq/metricq-go)

Go client library for [MetricQ](https://github.com/metricq/metricq). It provides
clients for publishing live data, subscribing to metrics, querying historic
data, implementing transformers, and exchanging management RPCs.

## Requirements

- Go 1.23 or newer
- a MetricQ manager and RabbitMQ server
- a token configured for the client type being used

## Installation

```sh
go get github.com/metricq/metricq-go
```

Import the package as `metricq`:

```go
import metricq "github.com/metricq/metricq-go"
```

All clients use `context.Context` for request deadlines and shutdown. Source,
Sink, History, and Transformer clients automatically restore their management
and data connections after transient connection loss.

## Clients

### Source

A source registers metrics and publishes timestamped values:

```go
ctx := context.Background()

source, err := metricq.NewSourceFromToken("source-go-example", "amqp://localhost")
if err != nil {
	log.Fatal(err)
}
defer source.Close()

if err := source.Connect(ctx); err != nil {
	log.Fatal(err)
}
if _, err := source.Register(ctx); err != nil {
	log.Fatal(err)
}
if err := source.DeclareMetrics(ctx, map[string]interface{}{
	"example.temperature": metricq.MetricMetadata{
		Description: "Example temperature",
		Unit:        "°C",
		Rate:        1,
	},
}); err != nil {
	log.Fatal(err)
}

metric := source.Metric("example.temperature")
if err := metric.Send(ctx, time.Now(), 21.5); err != nil {
	log.Fatal(err)
}
```

See [`examples/source`](examples/source) for a complete program.

### Sink

A sink subscribes to live metrics and delivers their data points through a Go
channel:

```go
ctx := context.Background()

sink, err := metricq.NewSink("sink-go-example", "amqp://localhost")
if err != nil {
	log.Fatal(err)
}
defer sink.Close()

if err := sink.Connect(ctx); err != nil {
	log.Fatal(err)
}

points := make(chan metricq.MetricDataPoint)
if err := sink.NotifyDataPoint(points); err != nil {
	log.Fatal(err)
}
if err := sink.Subscribe(ctx, ctx, []string{"example.temperature"}, time.Hour); err != nil {
	log.Fatal(err)
}

for point := range points {
	fmt.Printf("%s %s %g\n", point.Metric, point.Timestamp, point.Value)
}
```

See [`examples/sink`](examples/sink) for the command-line example.

### History

`HistoryClient` supports last-value, aggregate, aggregate-timeline, and flexible
timeline requests. It shares the management connection of an `Agent`:

```go
agent, err := metricq.NewAgent("history-go-example", "amqp://localhost")
if err != nil {
	log.Fatal(err)
}
defer agent.Close()

if err := agent.Connect(context.Background()); err != nil {
	log.Fatal(err)
}

history, err := metricq.NewHistoryClient(context.Background(), agent)
if err != nil {
	log.Fatal(err)
}
defer history.Close()

value, timestamp, _, err := history.RequestLastValue(
	context.Background(),
	"example.temperature",
)
```

See [`examples/history`](examples/history) for timeline and aggregate queries.

### Transformer

A transformer combines the source and sink protocols. It can subscribe to
input metrics, declare output metrics, publish transformed values, and handle
manager-initiated runtime reconfiguration. `NotifyDataChunk` preserves MetricQ
message boundaries; `NotifyDataPoint` provides individual points instead.

The normal lifecycle is:

1. Create and connect the transformer.
2. Install a data-point or data-chunk notification channel.
3. Call `Register` and apply the returned configuration.
4. Call `Subscribe`, then `DeclareMetrics`.
5. Publish individual points through a `TransformerMetric`, or publish a
   `DataChunk` with `Transformer.Send`.
6. Run `ServeConfig` to accept runtime configuration changes.

### Agent and RPC

`Agent` is the common management-plane client embedded by sources, sinks, and
transformers. Use it directly when implementing a custom MetricQ protocol or a
history client. Call `Connect` before making RPC requests and `Close` during
shutdown.

## Development

```sh
gofmt -w .
go vet ./...
go test -race ./...
go build ./...
```

`go.mod` and `go.sum` are the source of truth for dependencies. Run
`go mod tidy` after changing imports or dependency versions.
