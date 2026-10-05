package metricq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

// DBBinding maps an incoming data routing key to a stored/history metric.
type DBBinding struct {
	Name  string `json:"name"`
	Input string `json:"input,omitempty"`
}

// DBHandlers must be safe for concurrent configuration, data and history calls.
// Data must return nil only after the entire chunk is durable. Until it returns,
// the delivery remains unacknowledged and bounded AMQP prefetch applies pressure.
// A Data error stops Run and leaves the delivery in RabbitMQ for redelivery.
type DBHandlers struct {
	Configure func(context.Context, json.RawMessage) ([]DBBinding, error)
	Data      func(context.Context, string, *DataChunk) error
	// DataBatch, when set, replaces Data. It receives the deliveries already
	// buffered by AMQP prefetch together, so one durability barrier (such as a
	// single fsync) covers all of them. A batch waits at most 500 µs for further
	// deliveries. All messages are acknowledged together after it returns nil; an
	// error leaves the entire batch unacknowledged.
	DataBatch func(context.Context, []DataMessage) error
	History   func(context.Context, string, *HistoryRequest) (*HistoryResponse, error)
}

// DataMessage is one delivery of a DataBatch call.
type DataMessage struct {
	Input string
	Chunk *DataChunk
}

// maxDataBatchBytes bounds the encoded deliveries handed to one DataBatch call.
const maxDataBatchBytes = 16 << 20

// dataBatchLinger bounds how long a batch waits for further buffered deliveries.
const dataBatchLinger = 500 * time.Microsecond

type DBRegisterResponse struct {
	DataServerAddress string          `json:"dataServerAddress"`
	DataQueue         string          `json:"dataQueue"`
	HistoryQueue      string          `json:"historyQueue"`
	Config            json.RawMessage `json:"config"`
	Error             string          `json:"error"`
}
type DB struct {
	*Agent
	Prefetch int
	// MaxDataBatch bounds the deliveries per DataBatch call; zero means Prefetch.
	MaxDataBatch int
	// HistoryPrefetch is the maximum number of concurrently handled history
	// requests. Each worker owns a channel with prefetch one so a slow request
	// cannot monopolize a batch of deliveries.
	HistoryPrefetch int
	// MaxHistoryReplyBytes bounds an encoded history response. RabbitMQ closes
	// the channel on messages above its max_message_size (16 MiB by default
	// since 4.0); the unacknowledged request would then be redelivered and end
	// every session. Larger responses are replaced by an error response.
	MaxHistoryReplyBytes int
	configMu             sync.Mutex
}

// DefaultMaxHistoryReplyBytes leaves headroom below RabbitMQ's default
// max_message_size of 16 MiB.
const DefaultMaxHistoryReplyBytes = 15 << 20

func NewDB(token, server string) (*DB, error) {
	a, err := NewAgent(token, server)
	if err != nil {
		return nil, err
	}
	return &DB{Agent: a, Prefetch: 400, HistoryPrefetch: 8, MaxHistoryReplyBytes: DefaultMaxHistoryReplyBytes}, nil
}
func decodeDBRegistration(b []byte) (DBRegisterResponse, error) {
	var r DBRegisterResponse
	if err := json.Unmarshal(b, &r); err != nil {
		return r, err
	}
	if r.Error != "" {
		return r, fmt.Errorf("db.register: %s", r.Error)
	}
	if r.DataServerAddress == "" || r.DataQueue == "" || r.HistoryQueue == "" {
		return r, fmt.Errorf("invalid db.register response")
	}
	// Older managers encode config as a JSON string.
	if len(r.Config) > 0 && r.Config[0] == '"' {
		var s string
		if err := json.Unmarshal(r.Config, &s); err != nil {
			return r, err
		}
		r.Config = json.RawMessage(s)
	}
	return r, nil
}
func (db *DB) Register(ctx context.Context) (DBRegisterResponse, error) {
	b, err := db.Rpc(ctx, "metricq.management", "db.register", nil)
	if err != nil {
		return DBRegisterResponse{}, err
	}
	return decodeDBRegistration(b)
}
func (db *DB) Subscribe(ctx context.Context, bindings []DBBinding) error {
	b, err := db.Rpc(ctx, "metricq.management", "db.subscribe", struct {
		Metrics  []DBBinding `json:"metrics"`
		Metadata bool        `json:"metadata"`
	}{bindings, false})
	if err != nil {
		return err
	}
	var r struct {
		Error string `json:"error"`
	}
	if err = json.Unmarshal(b, &r); err != nil {
		return err
	}
	if r.Error != "" {
		return fmt.Errorf("db.subscribe: %s", r.Error)
	}
	return nil
}
func (db *DB) configure(ctx context.Context, raw json.RawMessage, h DBHandlers) error {
	db.configMu.Lock()
	defer db.configMu.Unlock()
	bindings, err := h.Configure(ctx, raw)
	if err != nil {
		return err
	}
	subscribeCtx, done := context.WithTimeout(ctx, subscribeTimeout(len(bindings)))
	defer done()
	return db.Subscribe(subscribeCtx, bindings)
}

// subscribeTimeout allows for the manager binding every metric to the data
// and history queues, one AMQP round trip each (about 25 s for 1500 metrics
// on a development broker).
func subscribeTimeout(metrics int) time.Duration {
	return time.Minute + time.Duration(metrics)*50*time.Millisecond
}

// subscribeUntilDone repeats a subscription with backoff until it succeeds or
// ctx ends. A failure does not interrupt consumption: the durable queues keep
// their bindings from the previous run.
func subscribeUntilDone(ctx context.Context, metrics int, subscribe func(context.Context) error, backoff time.Duration) {
	for {
		attempt, done := context.WithTimeout(ctx, subscribeTimeout(metrics))
		err := subscribe(attempt)
		done()
		if err == nil || ctx.Err() != nil {
			return
		}
		log.Printf("db.subscribe of %d metrics failed: %v; retrying in %s", metrics, err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, time.Minute)
	}
}

// Run owns management/data connections, reconnects transport failures and blocks
// until cancellation or an application ingestion failure. History remains active
// while Data blocks on a WAL high watermark.
func (db *DB) Run(ctx context.Context, h DBHandlers) error {
	if h.Configure == nil || (h.Data == nil && h.DataBatch == nil) || h.History == nil || db.Prefetch < 1 || db.HistoryPrefetch < 1 || db.MaxDataBatch < 0 || db.MaxHistoryReplyBytes < 0 {
		return fmt.Errorf("invalid database handlers or prefetch")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := db.Connect(ctx); err != nil {
		return err
	}
	defer db.Agent.Close()
	configRequests := make(chan amqp.Delivery, 8)
	db.NotifyRPC("config", configRequests)
	defer db.UnregisterRPC("config")
	var workers sync.WaitGroup
	workers.Add(2)
	defer func() { cancel(); workers.Wait() }()
	go func() { defer workers.Done(); db.ServeDiscover(ctx, "metricq-go/db") }()
	go func() {
		defer workers.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-configRequests:
				requestCtx, done := context.WithTimeout(ctx, 30*time.Second)
				err := db.configure(requestCtx, msg.Body, h)
				response := map[string]any{}
				if err != nil {
					response["error"] = err.Error()
				}
				_ = db.SendRpcResponse(requestCtx, msg, response)
				done()
			}
		}
	}()
	delay := time.Second
	for ctx.Err() == nil {
		requestCtx, done := context.WithTimeout(ctx, 30*time.Second)
		reg, err := db.Register(requestCtx)
		var bindings []DBBinding
		if err == nil {
			db.configMu.Lock()
			bindings, err = h.Configure(requestCtx, reg.Config)
			db.configMu.Unlock()
		}
		done()
		if err == nil {
			// Like the C++ client, consume as soon as the queues exist instead of
			// waiting for the manager to bind every metric: the durable queues
			// keep their bindings from the previous run, and a backlog drains
			// meanwhile. New metrics deliver once their binding exists.
			subscribeCtx, stopSubscribe := context.WithCancel(ctx)
			go subscribeUntilDone(subscribeCtx, len(bindings), func(ctx context.Context) error { return db.Subscribe(ctx, bindings) }, time.Second)
			err = db.session(ctx, reg, h)
			stopSubscribe()
		}
		if f, ok := err.(*dbApplicationError); ok {
			return f.err
		}
		if ctx.Err() != nil {
			break
		}
		log.Printf("database transport unavailable: %v; retrying in %s", err, delay)
		select {
		case <-ctx.Done():
		case <-time.After(delay):
		}
		delay = min(delay*2, 8*time.Second)
	}
	return ctx.Err()
}

// encodeHistoryReply marshals a history response, replacing one larger than
// limit (if positive) by an error response the broker accepts.
func encodeHistoryReply(resp *HistoryResponse, metric string, limit int) ([]byte, error) {
	b, err := proto.Marshal(resp)
	if err != nil || limit <= 0 || len(b) <= limit {
		return b, err
	}
	return proto.Marshal(&HistoryResponse{Metric: metric, Error: fmt.Sprintf("history response of %d bytes exceeds the limit of %d bytes; request a shorter range or a larger interval", len(b), limit)})
}

type dbApplicationError struct{ err error }

func (e *dbApplicationError) Error() string { return e.err.Error() }
func (db *DB) session(ctx context.Context, reg DBRegisterResponse, h DBHandlers) error {
	address, err := deriveHistoryDataURL(db.Server, reg.DataServerAddress)
	if err != nil {
		return err
	}
	conn, err := amqp.Dial(address.String())
	if err != nil {
		return err
	}
	defer conn.Close()
	data, err := conn.Channel()
	if err != nil {
		return err
	}
	defer data.Close()
	if err = data.Qos(db.Prefetch, 0, false); err != nil {
		return err
	}
	deliveries, err := data.Consume(reg.DataQueue, "", false, true, false, false, nil)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	errors := make(chan error, db.HistoryPrefetch+1)
	defer func() { cancel(); _ = conn.Close(); wg.Wait() }()
	wg.Add(1)
	if h.DataBatch != nil {
		limit := db.MaxDataBatch
		if limit == 0 || limit > db.Prefetch {
			limit = db.Prefetch
		}
		go func() { defer wg.Done(); errors <- consumeDBDataBatches(ctx, deliveries, limit, h.DataBatch) }()
	} else {
		go func() { defer wg.Done(); errors <- consumeDBData(ctx, deliveries, h.Data) }()
	}
	for worker := 0; worker < db.HistoryPrefetch; worker++ {
		history, err := conn.Channel()
		if err != nil {
			return err
		}
		defer history.Close()
		if err := history.Qos(1, 0, false); err != nil {
			return err
		}
		// Confirm each reply on its own channel before ACKing its request.
		if err := history.Confirm(false); err != nil {
			return err
		}
		confirms := history.NotifyPublish(make(chan amqp.Confirmation, 1))
		requests, err := history.Consume(reg.HistoryQueue, "", false, false, false, false, nil)
		if err != nil {
			return err
		}
		wg.Add(1)
		go func() { defer wg.Done(); errors <- db.consumeHistory(ctx, history, requests, confirms, h.History) }()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errors:
		return err
	}
}
func consumeDBData(ctx context.Context, deliveries <-chan amqp.Delivery, handler func(context.Context, string, *DataChunk) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("data consumer closed")
			}
			chunk := new(DataChunk)
			if err := proto.Unmarshal(msg.Body, chunk); err != nil {
				return &dbApplicationError{fmt.Errorf("invalid DataChunk for %s: %w", msg.RoutingKey, err)}
			}
			if len(chunk.Value) != len(chunk.TimeDelta) {
				return &dbApplicationError{fmt.Errorf("mismatched DataChunk arrays for %s", msg.RoutingKey)}
			}
			if err := handler(ctx, msg.RoutingKey, chunk); err != nil {
				return &dbApplicationError{err}
			}
			if err := msg.Ack(false); err != nil {
				return err
			}
		}
	}
}

// consumeDBDataBatches takes one delivery, then everything already buffered up
// to the limits. A multiple ACK of the last delivery covers the whole batch:
// deliveries are consumed in order and every earlier one is already ACKed.
func consumeDBDataBatches(ctx context.Context, deliveries <-chan amqp.Delivery, limit int, handler func(context.Context, []DataMessage) error) error {
	for {
		var batch []amqp.Delivery
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("data consumer closed")
			}
			batch = append(batch, msg)
		}
		size := len(batch[0].Body)
		closed := false
		// The AMQP client hands over buffered deliveries through an unbuffered
		// channel, so it looks empty right after each receive. A short linger
		// collects what is already prefetched without waiting for new traffic.
		linger := time.NewTimer(dataBatchLinger)
	drain:
		for len(batch) < limit && size < maxDataBatchBytes {
			select {
			case msg, ok := <-deliveries:
				if !ok {
					closed = true
					break drain
				}
				batch = append(batch, msg)
				size += len(msg.Body)
			case <-linger.C:
				break drain
			}
		}
		linger.Stop()
		messages := make([]DataMessage, len(batch))
		for i, msg := range batch {
			chunk := new(DataChunk)
			if err := proto.Unmarshal(msg.Body, chunk); err != nil {
				return &dbApplicationError{fmt.Errorf("invalid DataChunk for %s: %w", msg.RoutingKey, err)}
			}
			if len(chunk.Value) != len(chunk.TimeDelta) {
				return &dbApplicationError{fmt.Errorf("mismatched DataChunk arrays for %s", msg.RoutingKey)}
			}
			messages[i] = DataMessage{Input: msg.RoutingKey, Chunk: chunk}
		}
		if err := handler(ctx, messages); err != nil {
			return &dbApplicationError{err}
		}
		if err := batch[len(batch)-1].Ack(true); err != nil {
			return err
		}
		if closed {
			return fmt.Errorf("data consumer closed")
		}
	}
}
func (db *DB) consumeHistory(ctx context.Context, ch *amqp.Channel, requests <-chan amqp.Delivery, confirms <-chan amqp.Confirmation, handler func(context.Context, string, *HistoryRequest) (*HistoryResponse, error)) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-requests:
			if !ok {
				return fmt.Errorf("history consumer closed")
			}
			if msg.ReplyTo == "" {
				if err := msg.Reject(false); err != nil {
					return err
				}
				continue
			}
			start := time.Now()
			req := new(HistoryRequest)
			err := proto.Unmarshal(msg.Body, req)
			var resp *HistoryResponse
			queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			if err == nil {
				resp, err = handler(queryCtx, msg.RoutingKey, req)
			}
			cancel()
			if err != nil {
				resp = &HistoryResponse{Metric: msg.RoutingKey, Error: err.Error()}
			}
			if resp == nil {
				resp = &HistoryResponse{Metric: msg.RoutingKey, Error: "empty database response"}
			}
			b, err := encodeHistoryReply(resp, msg.RoutingKey, db.MaxHistoryReplyBytes)
			if err != nil {
				return err
			}
			err = ch.PublishWithContext(ctx, "", msg.ReplyTo, false, false, amqp.Publishing{ContentType: "application/protobuf", CorrelationId: msg.CorrelationId, AppId: db.token, Body: b, Headers: amqp.Table{"x-request-duration": time.Since(start).Seconds()}})
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case c, ok := <-confirms:
				if !ok || !c.Ack {
					return fmt.Errorf("history reply not confirmed")
				}
			}
			if err = msg.Ack(false); err != nil {
				return err
			}
		}
	}
}
