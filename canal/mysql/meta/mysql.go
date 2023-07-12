package meta

import (
	"database/sql"
	"fmt"
	"github.com/gridsx/datagos/canal/mysql/filter"
	"github.com/gridsx/datagos/canal/mysql/mapper"

	"github.com/go-mysql-org/go-mysql/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/siddontang/go-log/log"
)

type MySQLInstance struct {
	Host     string `json:"host" yaml:"host"`
	Port     int    `json:"port" yaml:"port"`
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
	Database string `json:"database,omitempty" yaml:"database"`
}

func (i *MySQLInstance) ToDatasource() *sql.DB {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		i.Username, i.Password, i.Host, i.Port, i.Database))
	if err != nil {
		log.Error(err.Error())
		return nil
	}
	db.SetMaxOpenConns(5)
	pingErr := db.Ping()
	if pingErr != nil {
		log.Error(pingErr.Error())
		return nil
	}
	return db
}

// MySQLServerConfig binlog生产者配置
type MySQLServerConfig struct {
	MasterInfo MySQLInstance           `json:"masterInfo"`
	DumpFilter *filter.MySQLDumpFilter `json:"dumpFilter"`
}

// MySQLSinkerConfig binlog 消费者配置, 一个生产者，可以对应多个消费者
type MySQLSinkerConfig struct {
	DestDatasource MySQLInstance         `json:"destDatasource"`
	Filters        []filter.MySQLFilter  `json:"filters"`
	Mappings       []mapper.TableMapping `json:"mappings"`
	ErrorContinue  bool                  `json:"errorContinue"`
}

// MySQLSrcConfig 对应库字段 src 字段， 当src_type 为 SrcMySQL时
type MySQLSrcConfig struct {
	Position   *mysql.Position         `json:"position"`
	DumpConfig *filter.MySQLDumpFilter `json:"dumpConfig"`
	MySQLInstance
}

func (mc *MySQLSrcConfig) ToServerConfig() MySQLServerConfig {
	return MySQLServerConfig{
		MasterInfo: mc.MySQLInstance,
		DumpFilter: mc.DumpConfig,
	}
}
