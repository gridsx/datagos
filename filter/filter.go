package filter

import (
	"github.com/go-mysql-org/go-mysql/canal"
)

type Filter interface {
	Match(e *canal.RowsEvent) bool
}
