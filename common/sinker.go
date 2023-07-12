package common

import (
	"github.com/go-mysql-org/go-mysql/canal"
)

// Sinker 对应了binlog 的整体处理器
// 它可以是 MQ， MySQL， REDIS， 也可以是其他
type Sinker interface {
	// Enable 是否开启
	Enable() bool

	// Disable 设置停止
	Disable()

	// OnEvent 处理事件
	OnEvent(*canal.RowsEvent) error

	// ContinueOnError 错误是否继续
	ContinueOnError() bool
}

// Consumer , 是最小单元， 一个Sinker对应多个Consumer
type Consumer interface {
	Accept(e *canal.RowsEvent) error
	Name() string
}
