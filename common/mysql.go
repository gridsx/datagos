package common

import (
	"database/sql"
	"fmt"

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
