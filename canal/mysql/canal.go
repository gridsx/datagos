package mysql

import (
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/gridsx/datagos/canal/mysql/meta"
	"github.com/siddontang/go-log/log"
	"time"
)

// NewMySQLCanal 创建canal通道配置
func NewMySQLCanal(config string) (*canal.Canal, bool, error) {
	s := new(meta.MySQLSrcConfig)
	err := json.Unmarshal([]byte(config), s)
	if err != nil {
		return nil, false, err
	}

	c := s.ToServerConfig()
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", c.MasterInfo.Host, c.MasterInfo.Port)
	cfg.User = c.MasterInfo.Username
	cfg.Password = c.MasterInfo.Password
	cfg.HeartbeatPeriod = time.Second * 5
	cfg.DiscardNoMetaRowEvent = true
	cfg.TimestampStringLocation = time.UTC

	// TODO 补充 filter.
	// cfg.ExcludeTableRegex, 是否拉取这个日志
	// cfg.IncludeTableRegex， 是否拉取

	if c.DumpFilter != nil {
		// 全量配置相关
		cfg.Dump.ExecutionPath = `mysqldump`
		cfg.Dump.SkipMasterData = true
		cfg.Dump.Databases = c.DumpFilter.Databases
		cfg.Dump.Where = c.DumpFilter.Where
		cfg.Dump.TableDB = c.DumpFilter.TableDB
		cfg.Dump.Tables = c.DumpFilter.Tables
		cfg.Dump.IgnoreTables = c.DumpFilter.IgnoreTables
	} else {
		cfg.Dump.ExecutionPath = ""
	}
	cx, err := canal.NewCanal(cfg)
	if err != nil {
		log.Errorln(err)
		return nil, false, err
	}
	return cx, c.DumpFilter != nil, nil
}
