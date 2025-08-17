package extension

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/walterwanderley/sqlite"
)

type ProducerVirtualTable struct {
	client       *kgo.Client
	name         string
	logger       *slog.Logger
	loggerCloser io.Closer
}

func NewProducerVirtualTable(name string, opts []kgo.Opt, loggerDef string) (*ProducerVirtualTable, error) {
	kgo.NewClient(kgo.SeedBrokers())
	vtab := ProducerVirtualTable{
		name: name,
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
	}

	return &vtab, nil
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
	if vt.client == nil {
		return 0, fmt.Errorf("not connected to broker")
	}
	topic := values[0].Text()
	if topic == "" {
		return 0, fmt.Errorf("topic is required")
	}
	key := values[1].Blob()
	payload := values[2].Blob()

	res := vt.client.ProduceSync(context.Background(), &kgo.Record{
		Topic:     topic,
		Key:       key,
		Value:     payload,
		Timestamp: time.Now(),
	})

	return 0, res.FirstErr()
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
