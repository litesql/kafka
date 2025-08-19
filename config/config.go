package config

const (
	// Common config
	Brokers     = "brokers"       // Comma delimited list of seed brokers
	ClientID    = "client_id"     // ClientID (default sqlite)
	SaslType    = "sasl_type"     // SASL type: plain, sha256, sha512
	SaslUser    = "sasl_user"     // SASL user
	SaslPass    = "sasl_pass"     // SASL pass
	CertFile    = "cert_file"     // TLS: path to certificate file
	CertKeyFile = "cert_key_file" // TLS: path to .pem certificate key file
	CertCAFile  = "ca_file"       // TLS: path to CA certificate file
	Insecure    = "insecure"      // TLS: Insecure skip TLS verification
	Logger      = "logger"        // Log errors to "stdout, stderr or file:/path/to/log.txt"

	// Consumer module config
	TableName       = "table"             // table name where to store the incoming messages
	ConsumerGroup   = "consumer_group"    // Consumer group
	IsolationLevel  = "isolation_level"   // Fetch isolation level: 0 = read uncommitted (default), 1 = read committed
	AutoOffsetReset = "auto_offset_reset" // Determines the behavior of a consumer group when there is no valid committed offset for a partition. (latest, earliest, none)

	// Producer module config
	Timeout            = "timeout"              // Producer timeout default 10s
	MaxBufferedRecords = "max_buffered_records" // Max producer buffered records (default 10000)
	FlushOnCommit      = "flush_on_commit"      // true = Disable auto-flush and flush on commit/rollback, false = default (auto-fush)
	TransactionalID    = "transactional_id"     // Transactional ID
	TransactionTimeout = "transaction_timeout"  // Transaction timeout

	DefaultTableName        = "kafka_data"
	DefaultProducerVTabName = "kafka_producer"
	DefaultConsumerVTabName = "kafka_consumer"
	DefaultClientID         = "sqlite"
)
