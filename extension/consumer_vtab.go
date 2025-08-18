package extension

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/walterwanderley/sqlite"
)

type ConsumerVirtualTable struct {
	virtualTableName string
	tableName        string
	client           *kgo.Client
	subscriptions    []string
	stmt             *sqlite.Stmt
	stmtMu           sync.Mutex
	mu               sync.Mutex
	quitChan         chan struct{}
	logger           *slog.Logger
	loggerCloser     io.Closer
}

func NewConsumerVirtualTable(virtualTableName string, opts []kgo.Opt, tableName string, conn *sqlite.Conn, loggerDef string) (*ConsumerVirtualTable, error) {

	stmt, _, err := conn.Prepare(fmt.Sprintf(`INSERT INTO %s(topic, partition, key, value, headers, timestamp) VALUES(?, ?, ?, ?, ?, ?)`, tableName))
	if err != nil {
		return nil, err
	}

	vtab := ConsumerVirtualTable{
		virtualTableName: virtualTableName,
		tableName:        tableName,
		subscriptions:    make([]string, 0),
		stmt:             stmt,
	}

	logger, loggerCloser, err := loggerFromConfig(loggerDef)
	if err != nil {
		return nil, err
	}
	vtab.loggerCloser = loggerCloser
	vtab.logger = logger

	if len(opts) > 0 {
		client, err := kgo.NewClient(opts...)
		if err != nil {
			return nil, fmt.Errorf("creating new client: %w", err)
		}
		vtab.client = client

		vtab.quitChan = make(chan struct{})
		go func() {
			for {
				select {
				case <-vtab.quitChan:
					return
				default:
					fetches := client.PollFetches(context.Background())
					if fetches.IsClientClosed() {
						return
					}
					fetches.EachError(func(t string, p int32, err error) {
						vtab.logger.Error("fetch error", "topic", t, "partition", p, "error", err)
					})
					var rs []*kgo.Record
					fetches.EachRecord(func(r *kgo.Record) {
						err := vtab.insertRecord(r)
						if err != nil {
							return
						}
						rs = append(rs, r)
					})
					if len(rs) == 0 {
						continue
					}
					if err := client.CommitRecords(context.Background(), rs...); err != nil {
						vtab.logger.Error("commit records", "error", err)
					}
				}
			}
		}()
	}

	return &vtab, nil
}

func (vt *ConsumerVirtualTable) BestIndex(in *sqlite.IndexInfoInput) (*sqlite.IndexInfoOutput, error) {
	return &sqlite.IndexInfoOutput{EstimatedCost: 1000000}, nil
}

func (vt *ConsumerVirtualTable) Open() (sqlite.VirtualCursor, error) {
	if vt.client == nil {
		return nil, fmt.Errorf("not connected to the broker")
	}
	return newSubscriptionsCursor(vt.subscriptions), nil
}

func (vt *ConsumerVirtualTable) Disconnect() error {
	close(vt.quitChan)
	var err error
	if vt.loggerCloser != nil {
		err = vt.loggerCloser.Close()
	}
	if vt.client != nil {
		vt.client.Close()
	}

	return errors.Join(err, vt.stmt.Finalize())
}

func (vt *ConsumerVirtualTable) Destroy() error {
	return nil
}

func (vt *ConsumerVirtualTable) Insert(values ...sqlite.Value) (int64, error) {
	topic := values[0].Text()
	if topic == "" {
		return 0, fmt.Errorf("topic is required")
	}
	vt.mu.Lock()
	defer vt.mu.Unlock()
	if vt.contains(topic) {
		return 0, fmt.Errorf("already subscribed to the %q topic", topic)
	}
	vt.client.AddConsumeTopics(topic)
	vt.subscriptions = append(vt.subscriptions, topic)

	return 1, nil
}

func (vt *ConsumerVirtualTable) Update(_ sqlite.Value, _ ...sqlite.Value) error {
	return fmt.Errorf("UPDATE operations on %q is not supported", vt.virtualTableName)
}

func (vt *ConsumerVirtualTable) Replace(old sqlite.Value, new sqlite.Value, _ ...sqlite.Value) error {
	return fmt.Errorf("UPDATE operations on %q is not supported", vt.virtualTableName)
}

func (vt *ConsumerVirtualTable) Delete(v sqlite.Value) error {
	vt.mu.Lock()
	defer vt.mu.Unlock()
	index := v.Int()
	// slices are 0 based
	index--
	if index >= 0 && index < len(vt.subscriptions) {
		vt.client.PurgeTopicsFromClient(vt.subscriptions[index])
		vt.subscriptions = slices.Delete(vt.subscriptions, index, index+1)
	}
	return nil
}

func (vt *ConsumerVirtualTable) contains(topic string) bool {
	for _, subscription := range vt.subscriptions {
		if subscription == topic {
			return true
		}
	}
	return false
}

func (vt *ConsumerVirtualTable) insertRecord(rec *kgo.Record) error {
	var headersBuf bytes.Buffer
	if len(rec.Headers) > 0 {
		json.NewEncoder(&headersBuf).Encode(rec.Headers)
	}
	vt.stmtMu.Lock()
	defer vt.stmtMu.Unlock()
	err := vt.stmt.Reset()
	if err != nil {
		vt.logger.Error("reset statement", "error", err, "topic", rec.Topic, "partition", rec.Partition)
		return err
	}

	vt.stmt.BindText(1, rec.Topic)
	vt.stmt.BindInt64(2, int64(rec.Partition))
	vt.stmt.BindText(3, string(rec.Key))
	vt.stmt.BindText(4, string(rec.Value))
	vt.stmt.BindText(5, headersBuf.String())
	vt.stmt.BindText(6, rec.Timestamp.Format(time.RFC3339Nano))

	_, err = vt.stmt.Step()
	if err != nil {
		vt.logger.Error("insert data", "error", err, "topic", rec.Topic, "partition", rec.Partition, "key", string(rec.Key))
		return err
	}

	return nil
}

type subscriptionsCursor struct {
	data    []string
	current string // current row that the cursor points to
	rowid   int64  // current rowid .. negative for EOF
}

func newSubscriptionsCursor(data []string) *subscriptionsCursor {
	slices.Sort(data)
	return &subscriptionsCursor{
		data: data,
	}
}

func (c *subscriptionsCursor) Next() error {
	// EOF
	if c.rowid < 0 || int(c.rowid) >= len(c.data) {
		c.rowid = -1
		return sqlite.SQLITE_OK
	}
	// slices are zero based
	c.current = c.data[c.rowid]
	c.rowid += 1

	return sqlite.SQLITE_OK
}

func (c *subscriptionsCursor) Column(ctx *sqlite.VirtualTableContext, i int) error {
	if i == 0 {
		ctx.ResultText(c.current)
	}
	return nil
}

func (c *subscriptionsCursor) Filter(int, string, ...sqlite.Value) error {
	c.rowid = 0
	return c.Next()
}

func (c *subscriptionsCursor) Rowid() (int64, error) {
	return c.rowid, nil
}

func (c *subscriptionsCursor) Eof() bool {
	return c.rowid < 0
}

func (c *subscriptionsCursor) Close() error {
	return nil
}
