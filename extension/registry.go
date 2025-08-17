package extension

import (
	"github.com/walterwanderley/sqlite"

	"github.com/litesql/kafka/config"
)

func registerFunc(api *sqlite.ExtensionApi) (sqlite.ErrorCode, error) {
	if err := api.CreateModule(config.DefaultProducerVTabName, &ProducerModule{}, sqlite.ReadOnly(false)); err != nil {
		return sqlite.SQLITE_ERROR, err
	}
	if err := api.CreateModule(config.DefaultConsumerVTabName, &ConsumerModule{}, sqlite.ReadOnly(false)); err != nil {
		return sqlite.SQLITE_ERROR, err
	}
	if err := api.CreateFunction("kafka_info", &Info{}); err != nil {
		return sqlite.SQLITE_ERROR, err
	}

	return sqlite.SQLITE_OK, nil
}
