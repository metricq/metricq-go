package metricq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

type Source struct {
	*Agent
	connection *Connection
	exchange   string
}

type SourceRegisterResponse struct {
	DataServerAddress string          `json:"dataServerAddress"`
	DataExchange      string          `json:"dataExchange"`
	Config            json.RawMessage `json:"config"`
	Error             string          `json:"error,omitempty"`
}

func NewSource(token, server string) (*Source, error) {
	agent, err := NewAgent(token, server)
	if err != nil {
		return nil, fmt.Errorf("failed to create agent: %w", err)
	}
	return &Source{Agent: agent}, nil
}

func (resp *SourceRegisterResponse) parseDataServer(server *url.URL) (*url.URL, error) {
	dataServerAddress, err := url.Parse(resp.DataServerAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to parse dataServerAddress: %w", err)
	}

	if dataServerAddress.Scheme == "vhost" {
		dataServerAddress.Scheme = server.Scheme
		dataServerAddress.Host = server.Host
	}

    if server.User != nil && dataServerAddress.User == nil {
        dataServerAddress.User = server.User
    }

	return dataServerAddress, nil

}

func (src *Source) Register(ctx context.Context) (json.RawMessage, error) {
	response, err := src.Rpc(ctx, "metricq.management", "source.register", RpcMessage{"source.register"})
	if err != nil {
		return nil, fmt.Errorf("failed to send RPC: %w", err)
	}

	log.Printf("Received RPC response: %s", response)

	data := new(SourceRegisterResponse)
	err = json.Unmarshal(response, data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse RPC response: %w", err)
	}

	if data.Error != "" {
		return nil, fmt.Errorf("RPC failed: %s", data.Error)
	}

	src.connection = new(Connection)
	dataServer, err := data.parseDataServer(src.Server)
	if err != nil {
		return nil, err
	}
	err = src.connection.Connect(dataServer, fmt.Sprintf("data connection %s", src.token))
	if err != nil {
		return nil, fmt.Errorf("failed to connect data connection: %w", err)
	}
	src.exchange = data.DataExchange

	return data.Config, nil
}

type MetricMetadata struct {
	Description string  `json:"description"`
	Unit        string  `json:"unit"`
	Rate        float64 `json:"rate"`
}

type MetricDeclareMessage struct {
	RpcMessage
	Metrics map[string]interface{} `json:"metrics"`
}

func (src *Source) DeclareMetrics(ctx context.Context, metrics map[string]interface{}) error {
	response, err := src.Rpc(ctx, "metricq.management", "source.declare_metrics", MetricDeclareMessage{RpcMessage{"source.declare_metrics"}, metrics})
	if err != nil {
		return fmt.Errorf("failed to source.declare_metrics RPC: %w", err)
	}

	log.Printf("Received RPC response: %s", response)
	return nil
}

func (src *Source) Send(ctx context.Context, metric string, chunk *DataChunk) error {
	body, err := proto.Marshal(chunk)
	if err != nil {
		return fmt.Errorf("failed to marshal: %w", err)
	}

	packet := amqp.Publishing{
		ContentType: "application/protobuf",
		Body:        body,
	}

	// if src.connection.channel.IsClosed() {
	//     src.connection.channel, err = src.connection.connection.Channel()
	//     if err != nil {
	//         return fmt.Errorf("Failed to reopen channel: %v", err)
	//     }
	// }
	//
	err = src.connection.channel.PublishWithContext(ctx, src.exchange, metric, false, false, packet)
	if err != nil {
		return fmt.Errorf("failed to publish data message: %w", err)
	}

	return nil
}

func (src *Source) Metric(metric string) *SourceMetric {
	return &SourceMetric{src: src, name: metric, chunk: DataChunk{}}
}

type SourceMetric struct {
	src           *Source
	name          string
	chunkSize     int
	previous_time int64
	chunk         DataChunk
}

func (metric *SourceMetric) Send(ctx context.Context, time time.Time, value float64) error {
	metric.chunk.TimeDelta = append(metric.chunk.TimeDelta, time.UnixNano()-metric.previous_time)
	metric.chunk.Value = append(metric.chunk.Value, value)

	if len(metric.chunk.Value) > metric.chunkSize || metric.chunkSize == 0 {
		if err := metric.src.Send(ctx, metric.name, &metric.chunk); err != nil {
			return err
		}
		metric.chunk.Reset()
		metric.previous_time = 0
	}

	return nil
}

func (metric *SourceMetric) Name() string {
	return metric.name
}
