package metricq

import (
	"context"
	"encoding/json"
	"fmt"
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
	_, err := src.Rpc(ctx, "metricq.management", "source.declare_metrics", MetricDeclareMessage{RpcMessage{"source.declare_metrics"}, metrics})
	if err != nil {
		return fmt.Errorf("failed to source.declare_metrics RPC: %w", err)
	}

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
	return &SourceMetric{src: src, name: metric, chunkSize: 1, chunk: DataChunk{}}
}

type SourceMetric struct {
	src           *Source
	name          string
	chunkSize     int
	previous_time int64
	chunk         DataChunk
}

// Send sends the given data point (a timestamp-value pair) to the metric
// server. If chunking is enabled, only every chunkSize call will actually
// send a message.
func (metric *SourceMetric) Send(ctx context.Context, time time.Time, value float64) error {
	metric.chunk.TimeDelta = append(metric.chunk.TimeDelta, time.UnixNano()-metric.previous_time)
	metric.previous_time = time.UnixNano()
	metric.chunk.Value = append(metric.chunk.Value, value)

	if metric.chunkSize > 0 && len(metric.chunk.Value) >= metric.chunkSize {
		return metric.Flush(ctx)
	}

	return nil
}

// Flush send the currently buffered data points to the server.
// You only need to call this function, when you disabled chunking, i.e.,
// called Chunking(0).
func (metric *SourceMetric) Flush(ctx context.Context) error {
	if len(metric.chunk.Value) == 0 {
		return nil
	}

	if err := metric.src.Send(ctx, metric.name, &metric.chunk); err != nil {
		return err
	}

	metric.chunk.Reset()
	metric.previous_time = 0

	return nil
}

// Name returns the name of the metric
func (metric *SourceMetric) Name() string {
	return metric.name
}

// Chunking controls the usage of chunking for the metric.
// Setting it to a value greater than zero enables chunking with the given
// number as the amount of data points transmitted in one message.
// Setting it to zero disables chunking, i.e., you have to manually call Flush
func (metric *SourceMetric) Chunking(newChunkSize int) error {
	if newChunkSize < 0 {
		return fmt.Errorf("invalid chunk size: %v", newChunkSize)
	}

	metric.chunkSize = newChunkSize

	return nil
}
