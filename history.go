package metricq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
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

type HistoryAggregate struct {
	Minimum    float64
	Maximum    float64
	Sum        float64
	Count      uint64
	Integral   float64
	ActiveTime int64
}

func (agg *HistoryAggregate) Mean() (float64, error) {
	if agg.ActiveTime == 0 {
		return math.NaN(), fmt.Errorf("aggregate has zero active time")
	}

	return agg.Integral / float64(agg.ActiveTime), nil
}

func (agg *HistoryAggregate) MeanSum() (float64, error) {
	if agg.Count == 0 {
		return math.NaN(), fmt.Errorf("aggregate has zero values")
	}

	return agg.Sum / float64(agg.Count), nil
}

type HistoryDataResponse struct {
	Metric    string
	TimeDelta []int64
	Values    []float64
	Agg       []HistoryAggregate
	Error     string
	// Duration is the time the history server spent processing the request.
	// Zero if the server did not report it.
	Duration time.Duration
}

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

func (c *HistoryClient) Request(ctx context.Context, metric string, start, end time.Time, intervalMax time.Duration, requestType HistoryRequestType) (HistoryDataResponse, error) {
	c.connMu.RLock()
	channel := c.channel
	exchange := c.exchange
	queue := c.queue
	c.connMu.RUnlock()
	if channel == nil {
		return HistoryDataResponse{}, fmt.Errorf("history client is not connected")
	}

	correlationID := "mq-history-go-" + uuid.NewString()
	payload, err := proto.Marshal(&HistoryRequest{
		StartTime:   start.UnixNano(),
		EndTime:     end.UnixNano(),
		IntervalMax: int64(intervalMax),
		Type:        requestType,
	})
	if err != nil {
		return HistoryDataResponse{}, fmt.Errorf("encode history request: %w", err)
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
			return HistoryDataResponse{}, fmt.Errorf("publish history request: %w", err)
		}
	}

	select {
	case <-ctx.Done():
		return HistoryDataResponse{}, ctx.Err()
	case response := <-ch:
		decoded, err := unmarshalHistoryDataResponse(response.body)
		if err != nil {
			return HistoryDataResponse{}, err
		}
		decoded.Duration = response.duration
		return decoded, nil
	}
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
