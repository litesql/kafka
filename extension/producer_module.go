package extension

import (
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/walterwanderley/sqlite"

	"github.com/litesql/kafka/config"
)

type ProducerModule struct {
}

func (m *ProducerModule) Connect(conn *sqlite.Conn, args []string, declare func(string) error) (sqlite.VirtualTable, error) {
	virtualTableName := args[2]
	if virtualTableName == "" {
		virtualTableName = config.DefaultProducerVTabName
	}

	var (
		opts   = make([]kgo.Opt, 0)
		logger string
	)
	if len(args) > 3 {
		for _, opt := range args[3:] {
			k, v, ok := strings.Cut(opt, "=")
			if !ok {
				return nil, fmt.Errorf("invalid option: %q", opt)
			}
			k = strings.TrimSpace(k)
			v = sanitizeOptionValue(v)
			switch strings.ToLower(k) {
			case config.Logger:
				logger = v
			case config.Brokers:
				opts = append(opts, kgo.SeedBrokers(strings.Split(v, ",")...))
			case config.ClientID:
				opts = append(opts, kgo.ClientID(v))
			default:
				return nil, fmt.Errorf("unknown %q option", k)
			}
		}
	}

	vtab, err := NewProducerVirtualTable(virtualTableName, opts, logger)
	if err != nil {
		return nil, err
	}

	return vtab,
		declare("CREATE TABLE x(topic TEXT, key BLOB, value BLOB)")
}

func sanitizeOptionValue(v string) string {
	v = strings.TrimSpace(v)
	v = strings.TrimPrefix(v, "'")
	v = strings.TrimSuffix(v, "'")
	v = strings.TrimPrefix(v, "\"")
	v = strings.TrimSuffix(v, "\"")
	return os.ExpandEnv(v)
}
