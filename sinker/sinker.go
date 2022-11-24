package sinker

import (
	"github.com/go-mysql-org/go-mysql/canal"
)

// Sinker 对应了binlog 的整体处理器
// 它可以是 MQ， MySQL， REDIS， 也可以是其他
type Sinker interface {
	// 是否开启
	enable() bool

	// 设置停止
	disable()

	// 处理事件
	onEvent(*canal.RowsEvent) error

	// 错误是否继续
	continueOnError() bool
}

// Consumer , 是最小单元， 一个Sinker对应多个Consumer
type Consumer interface {
	Accept(e *canal.RowsEvent) error
	Name() string
}
