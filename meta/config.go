package meta

import (
	"github.com/gridsx/datagos/filter"
	"github.com/gridsx/datagos/mapper"
)

// ServerConfig binlog生产者配置
type ServerConfig struct {
	MasterInfo MySQLInstance      `json:"masterInfo"`
	DumpFilter *filter.DumpFilter `json:"dumpFilter"`
}

// MySQLSinkerConfig binlog 消费者配置, 一个生产者，可以对应多个消费者
type MySQLSinkerConfig struct {
	DestDatasource MySQLInstance         `json:"destDatasource"`
	Filters        []filter.Filter       `json:"filters"`
	Mappings       []mapper.TableMapping `json:"mappings"`
	ErrorContinue  bool                  `json:"errorContinue"`
}
