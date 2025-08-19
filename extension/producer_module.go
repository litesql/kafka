package extension

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

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
		opts = make([]kgo.Opt, 0)

		saslType string
		saslUser string
		saslPass string

		certFilePath    string
		certKeyFilePath string
		caFilePath      string
		insecure        *bool

		clientID       string
		timeout        time.Duration
		manualFlushing bool
		transactional  bool
		logger         string
		err            error
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
				clientID = v
			case config.Timeout:
				timeout, err = time.ParseDuration(v)
				if err != nil {
					return nil, fmt.Errorf("ivalid %q option value: %w", k, err)
				}
			case config.FlushOnCommit:
				opts = append(opts, kgo.ManualFlushing())
				manualFlushing = true
			case config.MaxBufferedRecords:
				i, err := strconv.Atoi(v)
				if err != nil {
					return nil, fmt.Errorf("invalid %q option value: %w", k, err)
				}
				opts = append(opts, kgo.MaxBufferedRecords(i))
			case config.TransactionalID:
				opts = append(opts, kgo.TransactionalID(v))
				if v != "" {
					transactional = true
				}
			case config.TransactionTimeout:
				timeout, err := time.ParseDuration(v)
				if err != nil {
					return nil, fmt.Errorf("invalid %q option value: %w", k, err)
				}
				opts = append(opts, kgo.TransactionTimeout(timeout))
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

	if timeout == 0 {
		timeout = 10 * time.Second
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

	if clientID == "" {
		clientID = config.DefaultClientID
	}
	opts = append(opts, kgo.ClientID(clientID))

	err = kgo.ValidateOpts(opts...)
	if err != nil {
		return nil, fmt.Errorf("invalid kafka options: %w", err)
	}

	var vtab sqlite.VirtualTable
	if transactional {
		vtab, err = NewTransactionalProducerVirtualTable(virtualTableName, opts, timeout, manualFlushing, logger)
		if err != nil {
			return nil, err
		}
	} else {
		vtab, err = NewProducerVirtualTable(virtualTableName, opts, timeout, manualFlushing, logger)
		if err != nil {
			return nil, err
		}
	}

	return vtab,
		declare("CREATE TABLE x(topic TEXT, key BLOB, value BLOB, headers JSONB)")
}

func sanitizeOptionValue(v string) string {
	v = strings.TrimSpace(v)
	v = strings.TrimPrefix(v, "'")
	v = strings.TrimSuffix(v, "'")
	v = strings.TrimPrefix(v, "\"")
	v = strings.TrimSuffix(v, "\"")
	return os.ExpandEnv(v)
}
