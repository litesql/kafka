package extension

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/walterwanderley/sqlite"
)

type ProducerVirtualTable struct {
	client         *kgo.Client
	name           string
	logger         *slog.Logger
	loggerCloser   io.Closer
	timeout        time.Duration
	manualFlushing bool
}

func NewProducerVirtualTable(name string, opts []kgo.Opt, timeout time.Duration, manualFlushing bool, loggerDef string) (*ProducerVirtualTable, error) {
	logger, loggerCloser, err := loggerFromConfig(loggerDef)
	if err != nil {
		return nil, err
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("creating new client: %w", err)
	}

	return &ProducerVirtualTable{
		name:           name,
		client:         client,
		manualFlushing: manualFlushing,
		logger:         logger,
		loggerCloser:   loggerCloser,
		timeout:        timeout,
	}, nil
}

func (vt *ProducerVirtualTable) BestIndex(in *sqlite.IndexInfoInput) (*sqlite.IndexInfoOutput, error) {
	return &sqlite.IndexInfoOutput{}, nil
}

func (vt *ProducerVirtualTable) Open() (sqlite.VirtualCursor, error) {
	return nil, fmt.Errorf("SELECT operations on %q is not supported", vt.name)
}

func (vt *ProducerVirtualTable) Disconnect() error {
	var err error
	if vt.loggerCloser != nil {
		err = vt.loggerCloser.Close()
	}
	if vt.client != nil {
		vt.client.Close()
	}
	return err
}

func (vt *ProducerVirtualTable) Destroy() error {
	return nil
}

func (vt *ProducerVirtualTable) Insert(values ...sqlite.Value) (int64, error) {
	topic := values[0].Text()
	if topic == "" {
		return 0, fmt.Errorf("topic is required")
	}
	key := values[1].Blob()
	payload := values[2].Blob()

	headersText := values[3].Text()
	var headers []kgo.RecordHeader
	if headersText != "" {
		var data map[string]string
		err := json.Unmarshal([]byte(headersText), &data)
		if err != nil {
			return 0, fmt.Errorf("invalid headers: %w", err)
		}
		for k, v := range data {
			headers = append(headers, kgo.RecordHeader{
				Key:   k,
				Value: []byte(v),
			})
		}
	}
	ctx := context.Background()
	var cancel func()
	if vt.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, vt.timeout)
		defer cancel()
	}
	res := vt.client.ProduceSync(ctx, &kgo.Record{
		Topic:     topic,
		Key:       key,
		Value:     payload,
		Timestamp: time.Now(),
		Headers:   headers,
	})
	err := res.FirstErr()
	if err != nil {
		return 0, fmt.Errorf("kafka producer: %w", err)
	}

	return 0, nil
}

func (vt *ProducerVirtualTable) Update(_ sqlite.Value, _ ...sqlite.Value) error {
	return fmt.Errorf("UPDATE operations on %q is not supported", vt.name)
}

func (vt *ProducerVirtualTable) Replace(old sqlite.Value, new sqlite.Value, _ ...sqlite.Value) error {
	return fmt.Errorf("UPDATE operations on %q is not supported", vt.name)
}

func (vt *ProducerVirtualTable) Delete(_ sqlite.Value) error {
	return fmt.Errorf("DELETE operations on %q is not supported", vt.name)
}

func (vt *ProducerVirtualTable) Begin() error {
	return nil
}

func (vt *ProducerVirtualTable) Commit() error {
	if vt.client == nil || !vt.manualFlushing {
		return nil
	}
	return vt.client.Flush(context.Background())
}

func (vt *ProducerVirtualTable) Rollback() error {
	if vt.client == nil || !vt.manualFlushing {
		return nil
	}
	return vt.client.Flush(context.Background())
}
