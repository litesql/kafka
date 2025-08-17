package config

const (
	// Common config
	Brokers  = "brokers" // Comma delimited list of seed brokers
	ClientID = "client_id"
	Logger   = "logger" // Log errors to "stdout, stderr or file:/path/to/log.txt"

	// Consumer module config
	TableName     = "table"          // table name where to store the incoming messages
	ConsumerGroup = "consumer_group" // Consumer group

	DefaultTableName        = "kafka_data"
	DefaultProducerVTabName = "kafka_producer"
	DefaultConsumerVTabName = "kafka_consumer"
)
