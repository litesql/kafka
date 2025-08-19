package extension

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"regexp"
	"strconv"
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
		opts = make([]kgo.Opt, 0)

		saslType string
		saslUser string
		saslPass string

		certFilePath    string
		certKeyFilePath string
		caFilePath      string
		insecure        *bool

		clientID  string
		tableName string
		logger    string
		err       error
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
			case config.ClientID:
				clientID = v
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
			case config.SaslType:
				if !validateSASLType(v) {
					return nil, fmt.Errorf("invalid %q option value. use plain, sha256 or sha512", k)
				}
				saslType = v
			case config.SaslUser:
				saslUser = v
			case config.SaslPass:
				saslPass = v
			case config.CertFile:
				certFilePath = v
			case config.CertKeyFile:
				certKeyFilePath = v
			case config.CertCAFile:
				caFilePath = v
			case config.Insecure:
				b, err := strconv.ParseBool(v)
				if err != nil {
					return nil, fmt.Errorf("invalid %q option: %v", k, err)
				}
				insecure = &b
			default:
				return nil, fmt.Errorf("unknown %q option", k)
			}
		}
	}

	var (
		useTLS    bool
		tlsConfig tls.Config
	)
	if insecure != nil {
		useTLS = true
		tlsConfig.InsecureSkipVerify = *insecure
	}
	if certFilePath != "" && certKeyFilePath != "" {
		useTLS = true
		clientCert, err := tls.LoadX509KeyPair(certFilePath, certKeyFilePath)
		if err != nil {
			return nil, fmt.Errorf("error loading client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{clientCert}
	}

	if caFilePath != "" {
		useTLS = true
		caCertPEM, err := os.ReadFile(caFilePath)
		if err != nil {
			return nil, fmt.Errorf("error loading CA certificate: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCertPEM) {
			return nil, fmt.Errorf("error appending CA certificate to pool")
		}
		tlsConfig.RootCAs = caCertPool
	}

	if useTLS {
		opts = append(opts, kgo.DialTLSConfig(&tlsConfig))
	}

	if saslType != "" {
		opts = append(opts, kgo.SASL(newSASLMechanism(saslType, saslUser, saslPass)))
	}

	if tableName == "" {
		tableName = config.DefaultTableName
	}

	if !tableNameValid(tableName) {
		return nil, fmt.Errorf("table name %q is invalid", tableName)
	}

	err = conn.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
	    topic TEXT,
		partition INTEGER,
		key TEXT,
		value BLOB,
		headers JSONB,		
		offset INTEGER,
		timestamp DATETIME
	)`, tableName), nil)
	if err != nil {
		return nil, fmt.Errorf("creating %q table: %w", tableName, err)
	}

	if clientID == "" {
		clientID = config.DefaultClientID
	}
	opts = append(opts, kgo.ClientID(clientID))

	err = kgo.ValidateOpts(opts...)
	if err != nil {
		return nil, fmt.Errorf("invalid kafka options: %w", err)
	}

	vtab, err := NewConsumerVirtualTable(virtualTableName, opts, tableName, conn, logger)
	if err != nil {
		return nil, err
	}
	return vtab, declare("CREATE TABLE x(topic TEXT)")
}
