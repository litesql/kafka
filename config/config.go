package config

const (
	// Common config
	Brokers  = "brokers" // Comma delimited list of seed brokers
	ClientID = "client_id"
	Logger   = "logger" // Log errors to "stdout, stderr or file:/path/to/log.txt"

	// Consumer module config
	TableName      = "table"           // table name where to store the incoming messages
	ConsumerGroup  = "consumer_group"  // Consumer group
	IsolationLevel = "isolation_level" // Fetch isolation level: 0 = read uncommitted (default), 1 = read committed

	// Producer module config
	MaxBufferedRecords = "max_buffered_records" // Max producer buffered records (default 10000)
	FlushOnCommit      = "flush_on_commit"      // true = Disable auto-flush and flush on commit/rollback, false = default (auto-fush)
	TransactionalID    = "transactional_id"     // Transactional ID
	TransactionTimeout = "transaction_timeout"  // Transaction timeout

	DefaultTableName        = "kafka_data"
	DefaultProducerVTabName = "kafka_producer"
	DefaultConsumerVTabName = "kafka_consumer"
)
