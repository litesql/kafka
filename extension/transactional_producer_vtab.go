package extension

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type TransactionalProducerVirtualTable struct {
	*ProducerVirtualTable
}

func NewTransactionalProducerVirtualTable(name string, opts []kgo.Opt, manualFlushing bool, loggerDef string) (*TransactionalProducerVirtualTable, error) {
	base, err := NewProducerVirtualTable(name, opts, manualFlushing, loggerDef)
	if err != nil {
		return nil, err
	}
	return &TransactionalProducerVirtualTable{
		ProducerVirtualTable: base,
	}, nil
}

func (vt *TransactionalProducerVirtualTable) Begin() error {
	err := vt.client.BeginTransaction()
	if err != nil {
		return fmt.Errorf("kafka begin: %w", err)
	}
	return nil
}

func (vt *TransactionalProducerVirtualTable) Commit() error {
	err := vt.client.Flush(context.Background())
	if err != nil {
		return fmt.Errorf("kafka flush: %w", err)
	}
	err = vt.client.EndTransaction(context.Background(), kgo.TryCommit)
	if err != nil {
		return fmt.Errorf("kafka commit: %w", err)
	}
	return nil
}

func (vt *TransactionalProducerVirtualTable) Rollback() error {
	err := vt.client.Flush(context.Background())
	if err != nil {
		return fmt.Errorf("kafka flush: %w", err)
	}
	err = vt.client.EndTransaction(context.Background(), kgo.TryAbort)
	if err != nil {
		return fmt.Errorf("kafka rollback: %w", err)
	}
	return nil
}
