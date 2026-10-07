package metricq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

type dbAck struct{ ack chan struct{} }

func (a dbAck) Ack(uint64, bool) error        { close(a.ack); return nil }
func (a dbAck) Nack(uint64, bool, bool) error { return nil }
func (a dbAck) Reject(uint64, bool) error     { return nil }
func TestDBAckOnlyAfterDurability(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ack := dbAck{make(chan struct{})}
	messages := make(chan amqp.Delivery, 1)
	b, _ := proto.Marshal(&DataChunk{TimeDelta: []int64{100}, Value: []float64{2}})
	messages <- amqp.Delivery{Acknowledger: ack, Body: b, RoutingKey: "x"}
	entered := make(chan struct{})
	durable := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- consumeDBData(ctx, messages, func(context.Context, string, *DataChunk) error { close(entered); <-durable; return nil })
	}()
	<-entered
	select {
	case <-ack.ack:
		t.Fatal("ACK before durability")
	default:
	}
	close(durable)
	select {
	case <-ack.ack:
	case <-time.After(time.Second):
		t.Fatal("no ACK after durability")
	}
	cancel()
	<-done
}
func TestDBFailureDoesNotAck(t *testing.T) {
	ack := dbAck{make(chan struct{})}
	messages := make(chan amqp.Delivery, 1)
	b, _ := proto.Marshal(&DataChunk{TimeDelta: []int64{100}, Value: []float64{2}})
	messages <- amqp.Delivery{Acknowledger: ack, Body: b}
	err := consumeDBData(context.Background(), messages, func(context.Context, string, *DataChunk) error { return errors.New("disk full") })
	if err == nil {
		t.Fatal("lost handler error")
	}
	select {
	case <-ack.ack:
		t.Fatal("ACK despite failure")
	default:
	}
}
func TestDBRegistration(t *testing.T) {
	for _, raw := range []string{`{"dataServerAddress":"vhost:/data","dataQueue":"data","historyQueue":"history","config":{"metrics":{}}}`, `{"dataServerAddress":"vhost:/data","dataQueue":"data","historyQueue":"history","config":"{\"metrics\":{}}"}`} {
		r, err := decodeDBRegistration([]byte(raw))
		if err != nil || string(r.Config) != `{"metrics":{}}` {
			t.Fatalf("%+v %v", r, err)
		}
	}
	if _, err := decodeDBRegistration([]byte(`{"error":"no config"}`)); err == nil {
		t.Fatal("ignored manager error")
	}
}

type recordingAck struct {
	acks chan [2]uint64
}

func (a recordingAck) Ack(tag uint64, multiple bool) error {
	m := uint64(0)
	if multiple {
		m = 1
	}
	a.acks <- [2]uint64{tag, m}
	return nil
}
func (a recordingAck) Nack(uint64, bool, bool) error { return nil }
func (a recordingAck) Reject(uint64, bool) error     { return nil }

func TestDBDataBatchAcksBufferedDeliveriesOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ack := recordingAck{make(chan [2]uint64, 8)}
	messages := make(chan amqp.Delivery, 8)
	for i := uint64(1); i <= 5; i++ {
		b, _ := proto.Marshal(&DataChunk{TimeDelta: []int64{int64(i)}, Value: []float64{float64(i)}})
		messages <- amqp.Delivery{Acknowledger: ack, Body: b, RoutingKey: "x", DeliveryTag: i}
	}
	batches := make(chan []DataMessage, 4)
	durable := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- consumeDBDataBatches(ctx, messages, 3, func(_ context.Context, m []DataMessage) error {
			batches <- m
			<-durable
			return nil
		})
	}()
	first := <-batches
	if len(first) != 3 || first[0].Input != "x" || first[2].Chunk.Value[0] != 3 {
		t.Fatalf("first batch %+v", first)
	}
	select {
	case a := <-ack.acks:
		t.Fatalf("ACK %v before durability", a)
	case <-time.After(20 * time.Millisecond):
	}
	durable <- struct{}{}
	if a := <-ack.acks; a != [2]uint64{3, 1} {
		t.Fatalf("first ACK %v", a)
	}
	second := <-batches
	if len(second) != 2 {
		t.Fatalf("second batch has %d deliveries", len(second))
	}
	durable <- struct{}{}
	if a := <-ack.acks; a != [2]uint64{5, 1} {
		t.Fatalf("second ACK %v", a)
	}
	cancel()
	<-done
}

func TestDBDataBatchFailureDoesNotAck(t *testing.T) {
	ack := recordingAck{make(chan [2]uint64, 4)}
	messages := make(chan amqp.Delivery, 2)
	for i := uint64(1); i <= 2; i++ {
		b, _ := proto.Marshal(&DataChunk{TimeDelta: []int64{100}, Value: []float64{2}})
		messages <- amqp.Delivery{Acknowledger: ack, Body: b, DeliveryTag: i}
	}
	err := consumeDBDataBatches(context.Background(), messages, 8, func(context.Context, []DataMessage) error { return errors.New("disk full") })
	if err == nil {
		t.Fatal("lost handler error")
	}
	select {
	case a := <-ack.acks:
		t.Fatalf("ACK %v despite failure", a)
	default:
	}
}

func TestSubscribeTimeoutScalesWithMetrics(t *testing.T) {
	if got := subscribeTimeout(0); got != time.Minute {
		t.Fatalf("no metrics: %s", got)
	}
	if got := subscribeTimeout(1500); got < 2*time.Minute {
		t.Fatalf("1500 metrics: %s, the manager needs 25-50 s per queue", got)
	}
}

func TestSubscribeUntilDoneRetriesUntilSuccess(t *testing.T) {
	calls := 0
	subscribeUntilDone(context.Background(), 3, func(ctx context.Context) error {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("subscription without deadline")
		}
		calls++
		if calls < 3 {
			return errors.New("manager busy")
		}
		return nil
	}, time.Millisecond)
	if calls != 3 {
		t.Fatalf("%d attempts", calls)
	}
}

func TestSubscribeUntilDoneStopsWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		subscribeUntilDone(ctx, 1, func(context.Context) error { return errors.New("down") }, time.Hour)
		close(done)
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("retry loop ignored cancellation")
	}
}

func TestOversizedHistoryReplyBecomesError(t *testing.T) {
	big := &HistoryResponse{Metric: "m"}
	for i := 0; i < 1000; i++ {
		big.TimeDelta = append(big.TimeDelta, 40e9)
		big.Aggregate = append(big.Aggregate, &HistoryResponse_Aggregate{Minimum: 1, Maximum: 2, Sum: 3, Count: 4, Integral: 5, ActiveTime: 6})
	}
	full, err := encodeHistoryReply(big, "m", 0)
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := encodeHistoryReply(big, "m", len(full)); len(b) != len(full) {
		t.Fatal("response at the limit was replaced")
	}
	b, err := encodeHistoryReply(big, "m", len(full)-1)
	if err != nil {
		t.Fatal(err)
	}
	var resp HistoryResponse
	if err = proto.Unmarshal(b, &resp); err != nil || resp.Error == "" || len(resp.TimeDelta) != 0 || resp.Metric != "m" {
		t.Fatalf("oversized response not replaced by an error: %v %+v", err, &resp)
	}
}

// A batch made durable while the session stops is still acknowledged, and
// the connection closes only after its consumer returned.
func TestStopSessionAcksDurableBatchBeforeClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ack := recordingAck{make(chan [2]uint64, 1)}
	messages := make(chan amqp.Delivery, 1)
	b, _ := proto.Marshal(&DataChunk{TimeDelta: []int64{1}, Value: []float64{1}})
	messages <- amqp.Delivery{Acknowledger: ack, Body: b, RoutingKey: "x", DeliveryTag: 1}
	entered := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = consumeDBDataBatches(ctx, messages, 1, func(context.Context, []DataMessage) error {
			close(entered)
			time.Sleep(50 * time.Millisecond) // the WAL fsync outlasts the stop signal
			return nil
		})
	}()
	<-entered
	closed := make(chan struct{})
	stopSession(cancel, &wg, func() {
		select {
		case <-ack.acks:
		default:
			t.Error("connection closed before the durable batch was acknowledged")
		}
		close(closed)
	}, time.Second)
	<-closed
}

// A handler that does not return is cut off after the grace period.
func TestStopSessionClosesAfterGrace(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	release := make(chan struct{})
	go func() { defer wg.Done(); <-release }()
	start := time.Now()
	stopSession(cancel, &wg, func() { close(release) }, 30*time.Millisecond)
	if d := time.Since(start); d < 30*time.Millisecond || d > time.Second {
		t.Fatalf("stopped after %v", d)
	}
}
