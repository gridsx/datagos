package meta

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gridsx/datagos/canal/mysql/filter"
	"github.com/gridsx/datagos/common"
)

// MySQLServerConfig binlog生产者配置
type MySQLServerConfig struct {
	MasterInfo common.MySQLInstance    `json:"masterInfo"`
	DumpFilter *filter.MySQLDumpFilter `json:"dumpFilter"`
}

// MySQLSrcConfig 对应库字段 src 字段， 当src_type 为 SrcMySQL时
type MySQLSrcConfig struct {
	Position   *mysql.Position         `json:"position"`
	DumpConfig *filter.MySQLDumpFilter `json:"dumpConfig"`
	common.MySQLInstance
}

func (mc *MySQLSrcConfig) ToServerConfig() MySQLServerConfig {
	return MySQLServerConfig{
		MasterInfo: mc.MySQLInstance,
		DumpFilter: mc.DumpConfig,
	}
}
