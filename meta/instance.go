package meta

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gridsx/datagos/filter"
)

type MySQLInstance struct {
	Host     string `json:"host" yaml:"host"`
	Port     int    `json:"port" yaml:"port"`
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
	Database string `json:"database,omitempty" yaml:"database"`
}

type InstanceInfo struct {
	Id         int       `json:"id"`
	State      int       `json:"state"`
	Position   string    `json:"position"`
	DumpConfig string    `json:"dumpConfig"`
	Created    time.Time `json:"created"`
	Updated    time.Time `json:"updated"`
	MySQLInstance
}

func (i *InstanceInfo) ToDumpFilter() *filter.DumpFilter {
	if len(i.DumpConfig) > 0 {
		dumpFilter := new(filter.DumpFilter)
		err := json.Unmarshal([]byte(i.DumpConfig), dumpFilter)
		if err != nil {
			log.Warnf("error converting dump filter: %s\n", i.DumpConfig)
			return nil
		}
		return dumpFilter
	}
	return nil
}

func (i *InstanceInfo) ToPosition() *mysql.Position {
	if len(i.Position) > 0 {
		pos := new(mysql.Position)
		err := json.Unmarshal([]byte(i.Position), pos)
		if err != nil {
			log.Warnf("error converting position: %s\n", i.DumpConfig)
			return nil
		}
		return pos
	}
	return nil
}

func (i *InstanceInfo) ToServerConfig() ServerConfig {
	return ServerConfig{
		MasterInfo: i.MySQLInstance,
		DumpFilter: i.ToDumpFilter(),
	}
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
