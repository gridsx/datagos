package store

import (
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gridsx/datagos/config"
	"github.com/siddontang/go-log/log"
)

var (
	localDb *sql.DB
	once    = sync.Once{}
	conf    = config.GetConf()
)

func init() {
	initDb()
}

func initDb() {
	once.Do(func() {
		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
			conf.Mysql.Username, conf.Mysql.Password, conf.Mysql.Host, conf.Mysql.Port, conf.Mysql.Database))
		if err != nil {
			log.Panic(err)
			return
		}
		db.SetMaxOpenConns(5)
		pingErr := db.Ping()
		if pingErr != nil {
			log.Println(pingErr.Error())
			return
		}
		localDb = db
	})
}

func GetDb() *sql.DB {
	if localDb != nil {
		return localDb
	}
	initDb()
	return localDb
}
