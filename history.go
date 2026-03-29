package metricq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	duration time.Duration
}

type HistoryRequestType = HistoryRequest_RequestType

const (
	HistoryRequestTypeAggregateTimeline = HistoryRequest_AGGREGATE_TIMELINE
	HistoryRequestTypeAggregate         = HistoryRequest_AGGREGATE
	HistoryRequestTypeLastValue         = HistoryRequest_LAST_VALUE
	HistoryRequestTypeFlexTimeline      = HistoryRequest_FLEX_TIMELINE
)

// TimelineResult is returned by RequestTimeline.
// Use a type switch to distinguish between raw values and aggregates:
//
//	switch r := result.(type) {
//	case *TimelineValues:     // raw value series
//	case *TimelineAggregates: // aggregate series
//	}
type TimelineResult interface{ timelineResult() }

// TimelineValues holds a raw value series returned by a flex timeline request.
type TimelineValues struct {
	Times    []time.Time
	Values   []float64
	Duration time.Duration
}

// TimelineAggregates holds an aggregate series returned by a flex timeline request.
type TimelineAggregates struct {
	Times    []time.Time
	Agg      []*HistoryResponse_Aggregate
	Duration time.Duration
}

func (*TimelineValues) timelineResult()     {}
func (*TimelineAggregates) timelineResult() {}

type HistoryClient struct {
	agent *Agent

	connMu   sync.RWMutex
	reconnMu sync.Mutex
	conn     *amqp.Connection
	channel  *amqp.Channel
	queue    string
	exchange string

	consumeCancel context.CancelFunc

	pendingMu sync.Mutex
	pending   map[string]chan pendingHistoryResponse

	closed       chan struct{}
	reconnecting atomic.Bool

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
		closed:  make(chan struct{}),
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
	c.reconnMu.Lock()
	defer c.reconnMu.Unlock()

	select {
	case <-c.closed:
		return fmt.Errorf("history client closed")
	default:
	}

	reg, err := c.registerHistory(ctx)
	if err != nil {
		return err
	}

	c.connMu.Lock()
	oldCancel := c.consumeCancel
	oldConn := c.conn
	oldChannel := c.channel
	c.consumeCancel = nil
	c.conn = nil
	c.channel = nil
	c.queue = ""
	c.exchange = ""
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
	c.conn = historyConn
	c.channel = historyCh
	c.queue = reg.HistoryQueue
	c.exchange = reg.HistoryExchange
	c.consumeCancel = consumeCancel
	c.connMu.Unlock()

	return nil
}

func (c *HistoryClient) startConsumer(ctx context.Context, channel *amqp.Channel, queue string) error {
	msgs, err := channel.Consume(queue, "", false, true, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume history queue: %w", err)
	}
	closeCh := channel.NotifyClose(make(chan *amqp.Error, 1))
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case amqpErr, ok := <-closeCh:
				if !ok {
					c.triggerReconnect("history channel closed")
					return
				}
				c.triggerReconnect(fmt.Sprintf("history channel closed: %v", amqpErr))
				return
			case msg, ok := <-msgs:
				if !ok {
					c.triggerReconnect("history consumer stopped")
					return
				}
				c.handleHistoryMessage(msg)
			}
		}
	}()
	return nil
}

func (c *HistoryClient) triggerReconnect(reason string) {
	select {
	case <-c.closed:
		return
	default:
	}
	if !c.reconnecting.CompareAndSwap(false, true) {
		return
	}
	log.Printf("history connection lost: %s", reason)

	go func() {
		defer c.reconnecting.Store(false)
		backoff := 500 * time.Millisecond
		for {
			select {
			case <-c.closed:
				return
			default:
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := c.reconnect(ctx)
			cancel()
			if err == nil {
				log.Printf("history connection restored")
				return
			}
			log.Printf("history reconnect failed: %v", err)
			time.Sleep(backoff)
			if backoff < 8*time.Second {
				backoff *= 2
			}
		}
	}()
}

func isChannelClosedError(err error) bool {
	if err == nil {
		return false
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "channel/connection is not open") ||
		strings.Contains(msg, "channel is not open") ||
		strings.Contains(msg, "connection is not open") ||
		strings.Contains(msg, "exception (504)")
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

	var dur time.Duration
	if raw, ok := msg.Headers["x-request-duration"]; ok {
		var secs float64
		switch v := raw.(type) {
		case float64:
			secs = v
		case int64:
			secs = float64(v)
		case int32:
			secs = float64(v)
		case string:
			secs, _ = strconv.ParseFloat(v, 64)
		}
		dur = time.Duration(secs * float64(time.Second))
	}

	select {
	case ch <- pendingHistoryResponse{body: msg.Body, duration: dur}:
	default:
	}
}

// deltasToTimes converts MetricQ's cumulative-delta timestamps to Go time.Time.
// TimeDelta[0] is the first absolute nanosecond timestamp; each subsequent
// value is the nanosecond delta from the previous one.
func deltasToTimes(deltas []int64) []time.Time {
	times := make([]time.Time, len(deltas))
	var absNS int64
	for i, d := range deltas {
		absNS += d
		times[i] = time.Unix(0, absNS)
	}
	return times
}

// Request sends a low-level history request and returns the raw protobuf
// response along with the server-reported processing duration.
func (c *HistoryClient) Request(ctx context.Context, metric string, start, end time.Time, intervalMax time.Duration, requestType HistoryRequestType) (*HistoryResponse, time.Duration, error) {
	c.connMu.RLock()
	channel := c.channel
	exchange := c.exchange
	queue := c.queue
	c.connMu.RUnlock()
	if channel == nil {
		return nil, 0, fmt.Errorf("history client is not connected")
	}

	correlationID := "mq-history-go-" + uuid.NewString()
	payload, err := proto.Marshal(&HistoryRequest{
		StartTime:   start.UnixNano(),
		EndTime:     end.UnixNano(),
		IntervalMax: int64(intervalMax),
		Type:        requestType,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("encode history request: %w", err)
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
	publishErr := channel.PublishWithContext(ctx, exchange, metric, true, false, msg)
	if publishErr != nil {
		err := publishErr
		if isChannelClosedError(err) {
			reconnectCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			reconnectErr := c.reconnect(reconnectCtx)
			cancel()
			if reconnectErr == nil {
				c.connMu.RLock()
				retryChannel := c.channel
				retryExchange := c.exchange
				retryQueue := c.queue
				c.connMu.RUnlock()
				msg.ReplyTo = retryQueue
				if retryChannel != nil {
					if retryErr := retryChannel.PublishWithContext(ctx, retryExchange, metric, true, false, msg); retryErr == nil {
						err = nil
					} else {
						err = retryErr
					}
				}
			}
			c.triggerReconnect(fmt.Sprintf("history publish failed: %v", err))
		}
		if err != nil {
			return nil, 0, fmt.Errorf("publish history request: %w", err)
		}
	}

	select {
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	case response := <-ch:
		wireResp := &HistoryResponse{}
		if err := proto.Unmarshal(response.body, wireResp); err != nil {
			return nil, 0, fmt.Errorf("decode history response: %w", err)
		}
		return wireResp, response.duration, nil
	}
}

// RequestTimeline requests a flex timeline for the given metric and time range.
// The server decides whether to return raw values or aggregates based on the
// requested resolution. Use a type switch on the returned TimelineResult to
// distinguish between *TimelineValues and *TimelineAggregates.
func (c *HistoryClient) RequestTimeline(ctx context.Context, metric string, start, end time.Time, intervalMax time.Duration) (TimelineResult, error) {
	resp, dur, err := c.Request(ctx, metric, start, end, intervalMax, HistoryRequestTypeFlexTimeline)
	if err != nil {
		return nil, err
	}
	times := deltasToTimes(resp.GetTimeDelta())
	if aggs := resp.GetAggregate(); len(aggs) > 0 {
		return &TimelineAggregates{Times: times, Agg: aggs, Duration: dur}, nil
	}
	return &TimelineValues{Times: times, Values: resp.GetValue(), Duration: dur}, nil
}

// RequestAggregate requests a single aggregate over the given time range.
func (c *HistoryClient) RequestAggregate(ctx context.Context, metric string, start, end time.Time) (*HistoryResponse_Aggregate, time.Duration, error) {
	resp, dur, err := c.Request(ctx, metric, start, end, 0, HistoryRequestTypeAggregate)
	if err != nil {
		return nil, 0, err
	}
	aggs := resp.GetAggregate()
	if len(aggs) == 0 {
		return nil, dur, fmt.Errorf("no aggregate in response")
	}
	return aggs[0], dur, nil
}

// RequestLastValue requests the most recent value for the given metric.
func (c *HistoryClient) RequestLastValue(ctx context.Context, metric string) (float64, time.Time, time.Duration, error) {
	now := time.Now()
	resp, dur, err := c.Request(ctx, metric, now, now, 0, HistoryRequestTypeLastValue)
	if err != nil {
		return 0, time.Time{}, 0, err
	}
	values := resp.GetValue()
	deltas := resp.GetTimeDelta()
	if len(values) == 0 || len(deltas) == 0 {
		return 0, time.Time{}, dur, fmt.Errorf("no value in response")
	}
	return values[0], time.Unix(0, deltas[0]), dur, nil
}

func (c *HistoryClient) Close() error {
	var closeErr error
	c.closeOnce.Do(func() {
		close(c.closed)
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
