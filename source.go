package metricq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

type Source struct {
	*Agent
	connection    *Connection
	exchange      string
	mu            sync.Mutex
	registered    bool
	metrics       map[string]interface{}
	monitorCancel context.CancelFunc
	closed        chan struct{}
	closeOnce     sync.Once
	reconnecting  atomic.Bool
}

type SourceRegisterResponse struct {
	DataServerAddress string          `json:"dataServerAddress"`
	DataExchange      string          `json:"dataExchange"`
	Config            json.RawMessage `json:"config"`
	Error             string          `json:"error,omitempty"`
}

func NewSource(agent *Agent) (*Source, error) {
	if agent == nil {
		return nil, fmt.Errorf("nil agent")
	}
	src := &Source{
		Agent:   agent,
		metrics: map[string]interface{}{},
		closed:  make(chan struct{}),
	}
	agent.RegisterReconnectHook("source.register", src.reconnect)
	return src, nil
}

func NewSourceFromToken(token, server string) (*Source, error) {
	agent, err := NewAgent(token, server)
	if err != nil {
		return nil, fmt.Errorf("failed to create agent: %w", err)
	}
	return NewSource(agent)
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

	if err := src.applyRegisterResponse(data); err != nil {
		return nil, err
	}

	src.mu.Lock()
	src.registered = true
	src.mu.Unlock()

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

	src.mu.Lock()
	if src.metrics == nil {
		src.metrics = map[string]interface{}{}
	}
	for name, meta := range metrics {
		src.metrics[name] = meta
	}
	src.mu.Unlock()

	return nil
}

func (src *Source) applyRegisterResponse(data *SourceRegisterResponse) error {
	src.mu.Lock()
	old := src.connection
	oldCancel := src.monitorCancel
	src.connection = new(Connection)
	src.monitorCancel = nil
	src.mu.Unlock()

	if oldCancel != nil {
		oldCancel()
	}

	dataServer, err := data.parseDataServer(src.Server)
	if err != nil {
		return err
	}
	if err := src.connection.Connect(dataServer, fmt.Sprintf("data connection %s", src.token)); err != nil {
		return fmt.Errorf("failed to connect data connection: %w", err)
	}

	src.mu.Lock()
	src.exchange = data.DataExchange
	src.mu.Unlock()

	if old != nil {
		old.Close()
	}

	monitorCtx, monitorCancel := context.WithCancel(context.Background())
	src.mu.Lock()
	src.monitorCancel = monitorCancel
	src.mu.Unlock()

	closeCh := src.connection.connection.NotifyClose(make(chan *amqp.Error, 1))
	go func() {
		select {
		case <-monitorCtx.Done():
		case amqpErr, ok := <-closeCh:
			if !ok {
				src.triggerReconnect("source data connection closed")
			} else {
				src.triggerReconnect(fmt.Sprintf("source data connection closed: %v", amqpErr))
			}
		}
	}()

	return nil
}

func (src *Source) triggerReconnect(reason string) {
	select {
	case <-src.closed:
		return
	default:
	}
	if !src.reconnecting.CompareAndSwap(false, true) {
		return
	}
	log.Printf("source data connection lost: %s", reason)
	go func() {
		defer src.reconnecting.Store(false)
		backoff := 500 * time.Millisecond
		for {
			select {
			case <-src.closed:
				return
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := src.reconnect(ctx)
			cancel()
			if err == nil {
				log.Printf("source data connection restored")
				return
			}
			log.Printf("source reconnect failed: %v", err)
			time.Sleep(backoff)
			if backoff < 8*time.Second {
				backoff *= 2
			}
		}
	}()
}

func (src *Source) reconnect(ctx context.Context) error {
	src.mu.Lock()
	registered := src.registered
	metrics := make(map[string]interface{}, len(src.metrics))
	for name, meta := range src.metrics {
		metrics[name] = meta
	}
	src.mu.Unlock()

	if !registered {
		return nil
	}

	if _, err := src.Register(ctx); err != nil {
		return err
	}
	if len(metrics) == 0 {
		return nil
	}
	if err := src.DeclareMetrics(ctx, metrics); err != nil {
		return err
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

	src.mu.Lock()
	conn := src.connection
	exchange := src.exchange
	src.mu.Unlock()

	err = conn.channel.PublishWithContext(ctx, exchange, metric, false, false, packet)
	if err != nil {
		if isChannelClosedError(err) {
			src.triggerReconnect(fmt.Sprintf("source publish failed: %v", err))
		}
		return fmt.Errorf("failed to publish data message: %w", err)
	}

	return nil
}

func (src *Source) Close() {
	src.closeOnce.Do(func() {
		close(src.closed)
		src.mu.Lock()
		cancel := src.monitorCancel
		conn := src.connection
		src.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		if conn != nil {
			conn.Close()
		}
	})
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
