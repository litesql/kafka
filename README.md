# sqlite-kafka
SQLite Extension to integrate with Kafka brokers.

## Installation

Download **kafka** extension from the [releases page](https://github.com/litesql/kafka/releases).

### Compiling from source

- [Go 1.24+](https://go.dev) is required.

```sh
go build -ldflags="-s -w" -buildmode=c-shared -o kafka.so
```

## Basic usage

### Loading the extension

```sh
sqlite3

# Load the extension
.load ./kafka

# check version (optional)
SELECT kafka_info();
```

### Consumer

```sh
# Create a virtual table using KAFKA_CONSUMER to configure the connection to the broker
CREATE VIRTUAL TABLE temp.consumer USING kafka_consumer(brokers='localhost:44475', consumer_group='sqlite-kafka');

# Insert the topic name into the created virtual table to subscribe
INSERT INTO temp.consumer VALUES('my_topic');
```

Consumer table schema:

```sql
TABLE temp.consumer(
  topic TEXT
)
```

### Producer

```sh
# Create a virtual table using KAFKA_PRODUCER to configure the connection to the broker
CREATE VIRTUAL TABLE temp.producer USING kafka_producer(brokers='localhost:44475');

# Insert data into the created virtual table to produce messages
INSERT INTO temp.producer(topic, key, value) VALUES('my_topic', 'hello', 'world');
```

Producer table schema:

```sql
TABLE temp.producer(
  topic TEXT,   
  key BLOB, 
  value BLOB,
)
```

### Stored messages

```sh
# Set output mode (optional)
.mode qbox

# Query for the incoming messages
SELECT topic, key, value, timestamp FROM kafka_data;
┌────────────┬─────────┬─────────┬───────────────────────────────┐
│   topic    │   key   │  value  │           timestamp           │
├────────────┼─────────┼─────────┼───────────────────────────────┤
│ 'my_topic' │ 'hello' │ 'world' │ '2025-08-17T00:02:00.8-03:00' │
└────────────┴─────────┴─────────┴───────────────────────────────┘
```

Incoming messages are stored in tables according to the following schema:

```sql
TABLE kafka_data(
  topic TEXT,
  partition INTEGER,
  key TEXT,
  value BLOB,
  headers JSONB,		
  timestamp DATETIME
)
```

### Subscriptions management

Query the subscription virtual table (the virtual table created using **kafka_consumer**) to view all the active subscriptions for the current SQLite connection.

```sql
SELECT * FROM temp.consumer;
┌────────────┐
│   topic    │
├────────────┤
│ 'my_topic' │
└────────────┘
```

Delete the row to unsubscribe from the topic:

```sql
DELETE FROM temp.consumer WHERE topic = 'my_topic';
```

## Configuring

You can configure the connection to the broker by passing parameters to the VIRTUAL TABLE.

| Param | P/C/B | Description | Default |
|-------|------|-------------|---------|
| brokers | Both | Comma delimited list of seed brokers | |
| client_id | Both | Client ID sendo to all requests to kafka brokers | sqlite |
| flush_on_commit | Producer | Disable auto-flush and exec flush on commit | false |
| max_buffered_records | Producer | Max producer buffered records | 10000 |
| transactional_id | Producer | Transactional ID (if not empty enable transaction API support) | |
| transaction_timeout | Producer | Transaction timeout | 45s (45 seconds) |
| consumer_group | Consumer | Consumer group. Only for kafka_consumer | |
| isolation_level | Consumer | Fetch isolation level. 0 = read uncommitted (default), 1 = read committed
| table | Consumer | Name of the table where incoming messages will be stored. Only for kafka_consumer | kafka_data |
| logger | Both | Log errors to stdout, stderr or file:/path/to/file.log |
