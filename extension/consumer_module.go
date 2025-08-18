package extension

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/walterwanderley/sqlite"

	"github.com/litesql/kafka/config"
)

var tableNameValid = regexp.MustCompilePOSIX("^[a-zA-Z_][a-zA-Z0-9_.]*$").MatchString

type ConsumerModule struct {
}

func (m *ConsumerModule) Connect(conn *sqlite.Conn, args []string, declare func(string) error) (sqlite.VirtualTable, error) {
	virtualTableName := args[2]
	if virtualTableName == "" {
		virtualTableName = config.DefaultConsumerVTabName
	}

	var (
		opts      = make([]kgo.Opt, 0)
		tableName string
		logger    string
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
			case config.TableName:
				tableName = v
			case config.Logger:
				logger = v
			case config.Brokers:
				opts = append(opts, kgo.SeedBrokers(strings.Split(v, ",")...))
			case config.ConsumerGroup:
				opts = append(opts, kgo.ConsumerGroup(v))
			case config.IsolationLevel:
				switch v {
				case "0":
					opts = append(opts, kgo.FetchIsolationLevel(kgo.ReadUncommitted()))
				case "1":
					opts = append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))
				default:
					return nil, fmt.Errorf("invalid %q option value: must be 0 or 1", k)
				}
			default:
				return nil, fmt.Errorf("unknown %q option", k)
			}
		}
	}

	if tableName == "" {
		tableName = config.DefaultTableName
	}

	if !tableNameValid(tableName) {
		return nil, fmt.Errorf("table name %q is invalid", tableName)
	}

	err := conn.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
	    topic TEXT,
		partition INTEGER,
		key TEXT,
		value BLOB,
		headers JSONB,		
		timestamp DATETIME
	)`, tableName), nil)
	if err != nil {
		return nil, fmt.Errorf("creating %q table: %w", tableName, err)
	}

	vtab, err := NewConsumerVirtualTable(virtualTableName, opts, tableName, conn, logger)
	if err != nil {
		return nil, err
	}
	return vtab, declare("CREATE TABLE x(topic TEXT)")
}
