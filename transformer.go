package metricq

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

// Transformer combines the source and sink sides of the MetricQ transformer
// protocol. It intentionally exposes only MetricQ concepts; AMQP remains an
// implementation detail of this package.
type Transformer struct {
	*Agent
	lifecycleMu     sync.Mutex
	mu              sync.Mutex
	output          *Connection
	input           *Connection
	inputCancel     context.CancelFunc
	inputDone       chan struct{}
	dataExchange    string
	dataPointNotify chan<- MetricDataPoint
	dataChunkNotify chan<- MetricDataChunk
	metrics         map[string]interface{}
	closed          bool
	configHandler   TransformerConfigHandler
	reconnecting    atomic.Bool
}

type TransformerRegisterResponse struct {
	DataServerAddress string          `json:"dataServerAddress"`
	DataExchange      string          `json:"dataExchange"`
	Config            json.RawMessage `json:"config"`
	Error             string          `json:"error,omitempty"`
}

type TransformerSubscribeRequest struct {
	RpcMessage
	Metrics  []string `json:"metrics"`
	Metadata bool     `json:"metadata"`
}

type TransformerSubscribeResponse struct {
	DataServerAddress string                            `json:"dataServerAddress"`
	DataQueue         string                            `json:"dataQueue"`
	Metadata          map[string]map[string]interface{} `json:"metrics"`
	Error             string                            `json:"error,omitempty"`
}

type TransformerConfigHandler func(context.Context, json.RawMessage) error

type MetricDataChunk struct {
	Metric string
	Points []MetricDataPoint
}

func NewTransformer(token, server string) (*Transformer, error) {
	agent, err := NewAgent(token, server)
	if err != nil {
		return nil, fmt.Errorf("failed to create agent: %w", err)
	}
	transformer := &Transformer{Agent: agent, metrics: make(map[string]interface{})}
	agent.RegisterReconnectHook("transformer.register", transformer.refresh)
	return transformer, nil
}

func transformerDataServer(server *url.URL, address string) (*url.URL, error) {
	dataServer, err := url.Parse(address)
	if err != nil {
		return nil, fmt.Errorf("failed to parse dataServerAddress: %w", err)
	}
	if dataServer.Scheme == "vhost" {
		dataServer.Scheme = server.Scheme
		dataServer.Host = server.Host
	}
	if server.User != nil && dataServer.User == nil {
		dataServer.User = server.User
	}
	return dataServer, nil
}

// Register registers the transformer and returns its manager-provided config.
func (t *Transformer) Register(ctx context.Context) (json.RawMessage, error) {
	response, err := t.Rpc(ctx, "metricq.management", "transformer.register", RpcMessage{"transformer.register"})
	if err != nil {
		return nil, fmt.Errorf("transformer.register failed: %w", err)
	}
	var data TransformerRegisterResponse
	if err := json.Unmarshal(response, &data); err != nil {
		return nil, fmt.Errorf("failed to parse transformer.register response: %w", err)
	}
	if data.Error != "" {
		return nil, fmt.Errorf("transformer.register failed: %s", data.Error)
	}
	t.mu.Lock()
	t.dataExchange = data.DataExchange
	t.mu.Unlock()
	return data.Config, nil
}

func (t *Transformer) NotifyDataPoint(ch chan<- MetricDataPoint) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dataPointNotify != nil {
		return fmt.Errorf("another data point channel is already registered")
	}
	t.dataPointNotify = ch
	return nil
}

// NotifyDataChunk preserves MetricQ message boundaries. Transformer
// implementations that emulate chunk-at-a-time processing should prefer it to
// NotifyDataPoint.
func (t *Transformer) NotifyDataChunk(ch chan<- MetricDataChunk) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dataChunkNotify != nil {
		return fmt.Errorf("another data chunk channel is already registered")
	}
	t.dataChunkNotify = ch
	return nil
}

// ServeConfig handles manager-initiated runtime reconfiguration requests.
// The handler must rebuild the inputs and declare the outputs before returning.
func (t *Transformer) ServeConfig(ctx context.Context, handler TransformerConfigHandler) {
	t.mu.Lock()
	t.configHandler = handler
	t.mu.Unlock()
	requests := make(chan amqp.Delivery, 1)
	t.NotifyRPC("config", requests)
	defer t.UnregisterRPC("config")
	for {
		select {
		case <-ctx.Done():
			return
		case request, ok := <-requests:
			if !ok {
				return
			}
			var payload map[string]json.RawMessage
			if err := json.Unmarshal(request.Body, &payload); err != nil {
				_ = t.SendRpcResponse(ctx, request, map[string]string{"error": err.Error()})
				continue
			}
			delete(payload, "function")
			config, err := json.Marshal(payload)
			if err == nil {
				err = handler(ctx, config)
			}
			if err != nil {
				_ = t.SendRpcResponse(ctx, request, map[string]string{"error": err.Error()})
				continue
			}
			_ = t.SendRpcResponse(ctx, request, map[string]interface{}{})
		}
	}
}

func (t *Transformer) refresh(ctx context.Context) error {
	config, err := t.Register(ctx)
	if err != nil {
		return err
	}
	t.mu.Lock()
	handler := t.configHandler
	t.mu.Unlock()
	if handler == nil {
		return fmt.Errorf("transformer config handler is not installed")
	}
	return handler(ctx, config)
}

func (t *Transformer) triggerReconnect() {
	t.mu.Lock()
	closed := t.closed
	t.mu.Unlock()
	if closed || !t.reconnecting.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer t.reconnecting.Store(false)
		backoff := 500 * time.Millisecond
		for {
			t.mu.Lock()
			closed := t.closed
			t.mu.Unlock()
			if closed {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := t.refresh(ctx)
			cancel()
			if err == nil {
				return
			}
			time.Sleep(backoff)
			if backoff < 8*time.Second {
				backoff *= 2
			}
		}
	}()
}

// Subscribe replaces the current input subscription and starts consuming it.
func (t *Transformer) Subscribe(requestCtx, workerCtx context.Context, metrics []string) (map[string]map[string]interface{}, error) {
	t.lifecycleMu.Lock()
	defer t.lifecycleMu.Unlock()
	t.mu.Lock()
	notify, chunkNotify := t.dataPointNotify, t.dataChunkNotify
	t.mu.Unlock()
	if notify == nil && chunkNotify == nil {
		return nil, fmt.Errorf("NotifyDataPoint or NotifyDataChunk must be called before Subscribe")
	}
	request := TransformerSubscribeRequest{RpcMessage{"transformer.subscribe"}, metrics, true}
	response, err := t.Rpc(requestCtx, "metricq.management", "transformer.subscribe", request)
	if err != nil {
		return nil, fmt.Errorf("transformer.subscribe failed: %w", err)
	}
	var data TransformerSubscribeResponse
	if err := json.Unmarshal(response, &data); err != nil {
		return nil, fmt.Errorf("failed to parse transformer.subscribe response: %w", err)
	}
	if data.Error != "" {
		return nil, fmt.Errorf("transformer.subscribe failed: %s", data.Error)
	}
	dataServer, err := transformerDataServer(t.Server, data.DataServerAddress)
	if err != nil {
		return nil, err
	}
	input := new(Connection)
	if err := input.Connect(dataServer, fmt.Sprintf("transformer input %s", t.token)); err != nil {
		return nil, fmt.Errorf("failed to connect transformer input: %w", err)
	}
	output := new(Connection)
	if err := output.Connect(dataServer, fmt.Sprintf("transformer output %s", t.token)); err != nil {
		input.Close()
		return nil, fmt.Errorf("failed to connect transformer output: %w", err)
	}
	consumeCtx, consumeCancel := context.WithCancel(workerCtx)
	done := make(chan struct{})
	t.mu.Lock()
	oldInput, oldOutput := t.input, t.output
	oldCancel, oldDone := t.inputCancel, t.inputDone
	t.input = input
	t.output = output
	t.inputCancel = consumeCancel
	t.inputDone = done
	t.mu.Unlock()
	if oldCancel != nil {
		oldCancel()
	}
	if oldInput != nil {
		oldInput.Close()
	}
	if oldOutput != nil {
		oldOutput.Close()
	}
	if oldDone != nil {
		<-oldDone
	}
	go func() {
		defer close(done)
		t.consume(consumeCtx, input, data.DataQueue, notify, chunkNotify)
	}()
	return data.Metadata, nil
}

func (t *Transformer) consume(ctx context.Context, connection *Connection, queue string, notify chan<- MetricDataPoint, chunkNotify chan<- MetricDataChunk) {
	defer func() {
		t.mu.Lock()
		current := t.input == connection
		t.mu.Unlock()
		if current && ctx.Err() == nil {
			t.triggerReconnect()
		}
	}()
	channel, err := connection.connection.Channel()
	if err != nil {
		return
	}
	defer channel.Close()
	if err := channel.Qos(400, 0, false); err != nil {
		return
	}
	consumer, err := channel.Consume(queue, "", false, true, false, false, nil)
	if err != nil {
		return
	}
	for {
		select {
		case packet, ok := <-consumer:
			if !ok {
				return
			}
			var chunk DataChunk
			if err := proto.Unmarshal(packet.Body, &chunk); err != nil || len(chunk.TimeDelta) != len(chunk.Value) {
				_ = packet.Nack(false, false)
				continue
			}
			var previous int64
			points := make([]MetricDataPoint, 0, len(chunk.Value))
			for i, value := range chunk.Value {
				previous += chunk.TimeDelta[i]
				point := MetricDataPoint{Metric: packet.RoutingKey, Timestamp: time.Unix(0, previous), Value: value}
				points = append(points, point)
				if notify != nil {
					select {
					case notify <- point:
					case <-ctx.Done():
						return
					}
				}
			}
			if chunkNotify != nil {
				select {
				case chunkNotify <- MetricDataChunk{Metric: packet.RoutingKey, Points: points}:
				case <-ctx.Done():
					return
				}
			}
			_ = packet.Ack(false)
		case <-ctx.Done():
			return
		}
	}
}

func (t *Transformer) DeclareMetrics(ctx context.Context, metrics map[string]interface{}) error {
	payload := MetricDeclareMessage{RpcMessage{"transformer.declare_metrics"}, metrics}
	if _, err := t.Rpc(ctx, "metricq.management", "transformer.declare_metrics", payload); err != nil {
		return fmt.Errorf("transformer.declare_metrics failed: %w", err)
	}
	t.mu.Lock()
	for name, metadata := range metrics {
		t.metrics[name] = metadata
	}
	t.mu.Unlock()
	return nil
}

func (t *Transformer) Send(ctx context.Context, metric string, chunk *DataChunk) error {
	body, err := proto.Marshal(chunk)
	if err != nil {
		return fmt.Errorf("failed to marshal data chunk: %w", err)
	}
	t.mu.Lock()
	output, exchange := t.output, t.dataExchange
	t.mu.Unlock()
	if output == nil {
		return fmt.Errorf("transformer is not registered")
	}
	packet := amqp.Publishing{ContentType: "application/protobuf", Body: body}
	if err := output.channel.PublishWithContext(ctx, exchange, metric, false, false, packet); err != nil {
		t.triggerReconnect()
		return fmt.Errorf("failed to publish transformed data: %w", err)
	}
	return nil
}

func (t *Transformer) Metric(name string) *TransformerMetric {
	return &TransformerMetric{transformer: t, name: name, chunkSize: 1}
}

type TransformerMetric struct {
	transformer *Transformer
	name        string
	chunkSize   int
	previous    int64
	chunk       DataChunk
	mu          sync.Mutex
}

func (m *TransformerMetric) Chunking(size int) error {
	if size < 0 {
		return fmt.Errorf("invalid chunk size: %d", size)
	}
	m.mu.Lock()
	m.chunkSize = size
	m.mu.Unlock()
	return nil
}

func (m *TransformerMetric) Send(ctx context.Context, timestamp time.Time, value float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := timestamp.UnixNano()
	oldPrevious := m.previous
	m.chunk.TimeDelta = append(m.chunk.TimeDelta, now-m.previous)
	m.chunk.Value = append(m.chunk.Value, value)
	m.previous = now
	if m.chunkSize > 0 && len(m.chunk.Value) >= m.chunkSize {
		if err := m.flushLocked(ctx); err != nil {
			// Keep Send transactional so callers can safely retry the point.
			m.chunk.TimeDelta = m.chunk.TimeDelta[:len(m.chunk.TimeDelta)-1]
			m.chunk.Value = m.chunk.Value[:len(m.chunk.Value)-1]
			m.previous = oldPrevious
			return err
		}
	}
	return nil
}

func (m *TransformerMetric) Flush(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.flushLocked(ctx)
}

func (m *TransformerMetric) flushLocked(ctx context.Context) error {
	if len(m.chunk.Value) == 0 {
		return nil
	}
	if err := m.transformer.Send(ctx, m.name, &m.chunk); err != nil {
		return err
	}
	m.chunk.Reset()
	m.previous = 0
	return nil
}

func (t *Transformer) Close() {
	t.lifecycleMu.Lock()
	defer t.lifecycleMu.Unlock()
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	input, output := t.input, t.output
	inputCancel, inputDone := t.inputCancel, t.inputDone
	t.inputCancel, t.inputDone = nil, nil
	t.mu.Unlock()
	if inputCancel != nil {
		inputCancel()
	}
	if input != nil {
		input.Close()
	}
	if output != nil {
		output.Close()
	}
	if inputDone != nil {
		<-inputDone
	}
	t.Agent.Close()
}
