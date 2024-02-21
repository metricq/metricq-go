package metrigo

import (
	"context"
	"encoding/json"
	"log"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

type Source struct {
	connection *Connection
	agent      *Agent
}

type SourceRegisterMessage struct {
	Function string `json:"function"`
}

type SourceRegisterResponse struct {
	DataServerAddress string          `json:"dataServerAddress"`
	DataExchange      string          `json:"dataExchange"`
	Config            json.RawMessage `json:"config"`
}

func (resp *SourceRegisterResponse) parseDataServer(server string) string {
	if strings.HasPrefix(resp.DataServerAddress, "vhost:") {
		return server + strings.TrimPrefix(resp.DataServerAddress, "vhost:")
	} else {
		return resp.DataServerAddress
	}
}

func (src *Source) Register(ctx context.Context, agent *Agent) json.RawMessage {
	src.agent = agent
	response, err := agent.Rpc(ctx, "metricq.management", "source.register", SourceRegisterMessage{Function: "source.register"})
	if err != nil {
		log.Panicf("Failed to source.register RPC: %s", err)
	}

	log.Printf("Received RPC response: %s", response)

	data := new(SourceRegisterResponse)
	err = json.Unmarshal(response, data)
	if err != nil {
		log.Panicf("%s: %s", "Failed to parse RPC response", err)
	}

	src.connection = new(Connection)
	src.connection.Connect(data.parseDataServer(agent.Server))
	src.connection.exchange = data.DataExchange

	return data.Config
}

type MetricMetadata struct {
	Description string  `json:"description"`
	Unit        string  `json:"unit"`
	Rate        float64 `json:"rate"`
}

type MetricDeclareMessage struct {
	Function string                 `json:"function"`
	Metrics  map[string]interface{} `json:"metrics"`
}

func (src *Source) DeclareMetrics(ctx context.Context, metrics map[string]interface{}) {
	response, err := src.agent.Rpc(ctx, "metricq.management", "source.declare_metrics", MetricDeclareMessage{Function: "source.declare_metrics", Metrics: metrics})
	if err != nil {
		log.Panicf("Failed to source.declare_metrics RPC: %s", err)
	}

	log.Printf("Received RPC response: %s", response)
}

func (src *Source) Send(ctx context.Context, metric string, chunk *DataChunk) error {
	body, err := proto.Marshal(chunk)
	if err != nil {
		log.Panicf("Failed to marshal: %s", err)
		return err
	}

	packet := amqp.Publishing{
		ContentType: "application/protobuf",
		Body:        body,
	}

	err = src.connection.channel.PublishWithContext(ctx, src.connection.exchange, metric, false, false, packet)
	if err != nil {
		log.Panicf("Failed to publish data message: %s", err)
		return err
	}

	return nil
}

func (src *Source) Metric(metric string) SourceMetric {
	return SourceMetric{src: src, name: metric, chunk: DataChunk{}}
}

type SourceMetric struct {
	src           *Source
	name          string
	chunkSize     int
	previous_time int64
	chunk         DataChunk
}

func (metric *SourceMetric) Send(ctx context.Context, time int64, value float64) {
	metric.chunk.TimeDelta = append(metric.chunk.TimeDelta, time-metric.previous_time)
	metric.chunk.Value = append(metric.chunk.Value, value)

	if len(metric.chunk.Value) > metric.chunkSize || metric.chunkSize == 0 {
		metric.src.Send(ctx, metric.name, &metric.chunk)
		metric.chunk.Reset()
		metric.previous_time = 0
	}
}
