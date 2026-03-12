package metricq

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"

	uuid "github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

const defaultHistoryTimeout = 10 * time.Second

type historyRegisterResponse struct {
	DataServerAddress string `json:"dataServerAddress"`
	HistoryExchange   string `json:"historyExchange"`
	HistoryQueue      string `json:"historyQueue"`
}

type pendingHistoryResponse struct {
	body     []byte
	duration float64
}

type HistoryRequestType = HistoryRequest_RequestType

const (
	HistoryRequestTypeAggregateTimeline = HistoryRequest_AGGREGATE_TIMELINE
	HistoryRequestTypeAggregate         = HistoryRequest_AGGREGATE
	HistoryRequestTypeLastValue         = HistoryRequest_LAST_VALUE
	HistoryRequestTypeFlexTimeline      = HistoryRequest_FLEX_TIMELINE
)

type HistoryQuery struct {
	Metric      string
	StartTimeNS int64
	EndTimeNS   int64
	IntervalMax int64
	RequestType HistoryRequestType
	Timeout     time.Duration
}

type HistoryAggregate struct {
	Minimum    float64
	Maximum    float64
	Sum        float64
	Count      uint64
	Integral   float64
	ActiveTime int64
}

type HistoryDataResponse struct {
	Metric    string
	TimeDelta []int64
	Values    []float64
	Agg       []HistoryAggregate
	Error     string
}

type HistoryClient struct {
	agent *Agent

	connMu   sync.RWMutex
	conn     *amqp.Connection
	channel  *amqp.Channel
	queue    string
	exchange string

	consumeCancel context.CancelFunc

	pendingMu sync.Mutex
	pending   map[string]chan pendingHistoryResponse

	closeOnce sync.Once
}

func NewHistoryClient(ctx context.Context, agent *Agent) (*HistoryClient, error) {
	if agent == nil {
		return nil, fmt.Errorf("nil agent")
	}
	if agent.Server == nil {
		return nil, fmt.Errorf("agent server is nil")
	}
	if agent.connection == nil {
		return nil, fmt.Errorf("agent is not connected")
	}

	client := &HistoryClient{
		agent:   agent,
		pending: make(map[string]chan pendingHistoryResponse),
	}
	if err := client.reconnect(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	agent.RegisterReconnectHook("history.register", client.reconnect)
	return client, nil
}

func deriveHistoryDataURL(baseURL *url.URL, dataServerAddress string) (*url.URL, error) {
	dataURL, err := url.Parse(dataServerAddress)
	if err != nil {
		return nil, err
	}
	if dataURL.Scheme == "vhost" {
		dataURL.Scheme = baseURL.Scheme
		dataURL.Host = baseURL.Host
	}
	if dataURL.User == nil && baseURL.User != nil {
		dataURL.User = baseURL.User
	}
	return dataURL, nil
}

func (c *HistoryClient) registerHistory(ctx context.Context) (historyRegisterResponse, error) {
	respBytes, err := c.agent.Rpc(ctx, "metricq.management", "history.register", RpcMessage{Function: "history.register"})
	if err != nil {
		return historyRegisterResponse{}, fmt.Errorf("rpc history.register: %w", err)
	}

	var reg historyRegisterResponse
	if err := json.Unmarshal(respBytes, &reg); err != nil {
		return historyRegisterResponse{}, fmt.Errorf("parse history.register response: %w", err)
	}
	if reg.DataServerAddress == "" || reg.HistoryExchange == "" || reg.HistoryQueue == "" {
		return historyRegisterResponse{}, fmt.Errorf("invalid history.register response")
	}
	return reg, nil
}

func (c *HistoryClient) reconnect(ctx context.Context) error {
	reg, err := c.registerHistory(ctx)
	if err != nil {
		return err
	}

	historyURL, err := deriveHistoryDataURL(c.agent.Server, reg.DataServerAddress)
	if err != nil {
		return err
	}

	historyConn, err := amqp.Dial(historyURL.String())
	if err != nil {
		return fmt.Errorf("connect history amqp: %w", err)
	}
	historyCh, err := historyConn.Channel()
	if err != nil {
		_ = historyConn.Close()
		return fmt.Errorf("open history channel: %w", err)
	}

	if _, err := historyCh.QueueDeclarePassive(reg.HistoryQueue, false, false, false, false, nil); err != nil {
		_ = historyCh.Close()
		_ = historyConn.Close()
		return fmt.Errorf("declare passive history queue: %w", err)
	}

	consumeCtx, consumeCancel := context.WithCancel(context.Background())
	if err := c.startConsumer(consumeCtx, historyCh, reg.HistoryQueue); err != nil {
		consumeCancel()
		_ = historyCh.Close()
		_ = historyConn.Close()
		return err
	}

	c.connMu.Lock()
	oldCancel := c.consumeCancel
	oldConn := c.conn
	oldChannel := c.channel
	c.conn = historyConn
	c.channel = historyCh
	c.queue = reg.HistoryQueue
	c.exchange = reg.HistoryExchange
	c.consumeCancel = consumeCancel
	c.connMu.Unlock()

	if oldCancel != nil {
		oldCancel()
	}
	if oldChannel != nil {
		_ = oldChannel.Close()
	}
	if oldConn != nil {
		_ = oldConn.Close()
	}

	return nil
}

func (c *HistoryClient) startConsumer(ctx context.Context, channel *amqp.Channel, queue string) error {
	msgs, err := channel.Consume(queue, "", false, true, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume history queue: %w", err)
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				c.handleHistoryMessage(msg)
			}
		}
	}()
	return nil
}

func (c *HistoryClient) handleHistoryMessage(msg amqp.Delivery) {
	defer func() {
		_ = msg.Ack(false)
	}()
	if msg.CorrelationId == "" {
		return
	}

	c.pendingMu.Lock()
	ch, ok := c.pending[msg.CorrelationId]
	c.pendingMu.Unlock()
	if !ok {
		return
	}

	duration := -1.0
	if raw, ok := msg.Headers["x-request-duration"]; ok {
		switch v := raw.(type) {
		case string:
			if parsed, err := strconv.ParseFloat(v, 64); err == nil {
				duration = parsed
			}
		case float64:
			duration = v
		case int64:
			duration = float64(v)
		case int32:
			duration = float64(v)
		}
	}

	select {
	case ch <- pendingHistoryResponse{body: msg.Body, duration: duration}:
	default:
	}
}

func marshalHistoryQuery(req HistoryQuery) ([]byte, error) {
	wireReq := &HistoryRequest{
		StartTime:   req.StartTimeNS,
		EndTime:     req.EndTimeNS,
		IntervalMax: req.IntervalMax,
		Type:        req.RequestType,
	}
	return proto.Marshal(wireReq)
}

func unmarshalHistoryDataResponse(payload []byte) (HistoryDataResponse, error) {
	wireResp := &HistoryResponse{}
	if err := proto.Unmarshal(payload, wireResp); err != nil {
		return HistoryDataResponse{}, fmt.Errorf("decode history response: %w", err)
	}

	out := HistoryDataResponse{
		Metric:    wireResp.GetMetric(),
		TimeDelta: append([]int64(nil), wireResp.GetTimeDelta()...),
		Values:    append([]float64(nil), wireResp.GetValue()...),
		Error:     wireResp.GetError(),
	}
	if aggs := wireResp.GetAggregate(); len(aggs) > 0 {
		out.Agg = make([]HistoryAggregate, 0, len(aggs))
		for _, agg := range aggs {
			if agg == nil {
				continue
			}
			out.Agg = append(out.Agg, HistoryAggregate{
				Minimum:    agg.GetMinimum(),
				Maximum:    agg.GetMaximum(),
				Sum:        agg.GetSum(),
				Count:      agg.GetCount(),
				Integral:   agg.GetIntegral(),
				ActiveTime: agg.GetActiveTime(),
			})
		}
	}
	return out, nil
}

func (c *HistoryClient) Request(ctx context.Context, req HistoryQuery) (HistoryDataResponse, float64, error) {
	c.connMu.RLock()
	channel := c.channel
	exchange := c.exchange
	queue := c.queue
	c.connMu.RUnlock()
	if channel == nil {
		return HistoryDataResponse{}, 0, fmt.Errorf("history client is not connected")
	}

	correlationID := "mq-history-go-" + uuid.NewString()
	payload, err := marshalHistoryQuery(req)
	if err != nil {
		return HistoryDataResponse{}, 0, fmt.Errorf("encode history request: %w", err)
	}

	ch := make(chan pendingHistoryResponse, 1)
	c.pendingMu.Lock()
	c.pending[correlationID] = ch
	c.pendingMu.Unlock()
	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, correlationID)
		c.pendingMu.Unlock()
	}()

	msg := amqp.Publishing{
		Body:          payload,
		MessageId:     correlationID,
		CorrelationId: correlationID,
		ReplyTo:       queue,
		AppId:         c.agent.token,
	}
	if err := channel.PublishWithContext(ctx, exchange, req.Metric, true, false, msg); err != nil {
		return HistoryDataResponse{}, 0, fmt.Errorf("publish history request: %w", err)
	}

	timeout := req.Timeout
	if timeout <= 0 {
		timeout = defaultHistoryTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return HistoryDataResponse{}, 0, ctx.Err()
	case <-timer.C:
		return HistoryDataResponse{}, 0, fmt.Errorf("history request timeout")
	case response := <-ch:
		decoded, err := unmarshalHistoryDataResponse(response.body)
		if err != nil {
			return HistoryDataResponse{}, 0, err
		}
		return decoded, response.duration, nil
	}
}

func (c *HistoryClient) Close() error {
	var closeErr error
	c.closeOnce.Do(func() {
		if c.consumeCancel != nil {
			c.consumeCancel()
		}
		if c.channel != nil {
			if err := c.channel.Close(); err != nil {
				closeErr = err
			}
		}
		if c.conn != nil {
			if err := c.conn.Close(); err != nil && closeErr == nil {
				closeErr = err
			}
		}
	})
	return closeErr
}
