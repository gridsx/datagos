package filter

import (
	"github.com/go-mysql-org/go-mysql/canal"
)

// MySQLFilter 通用 MySQL事件过滤器
type MySQLFilter interface {
	Match(e *canal.RowsEvent) bool
}

// MySQLDumpFilter Dump的时候用來过滤库表的
type MySQLDumpFilter struct {
	Databases []string `json:"databases,omitempty"`

	// 这两个将会覆盖上面的数据库选项
	TableDB string   `json:"tableDB,omitempty"`
	Tables  []string `json:"tables,omitempty"`

	IgnoreTables []string `json:"ignoreTables,omitempty"`
	Where        string   `json:"where,omitempty"`
}
