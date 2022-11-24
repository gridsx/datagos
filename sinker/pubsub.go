package sinker

import "github.com/go-mysql-org/go-mysql/canal"

type PubSubSinker struct {
}

// 是否开启
func (s *PubSubSinker) enable() bool {
	return false
}

// 设置停止
func (s *PubSubSinker) disable() {}

// 处理事件
func (s *PubSubSinker) onEvent(*canal.RowsEvent) error {
	return nil
}

// 错误是否继续
func (s *PubSubSinker) continueOnError() bool {
	return false
}
