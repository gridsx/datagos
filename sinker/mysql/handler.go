package mysql

import (
	"github.com/gridsx/datagos/common"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/siddontang/go-log/log"
)

var (
	eventCount uint64 = 0
	lastNanos         = time.Now().Unix()
)

type MySQLBinlogHandler struct {
	canal.DummyEventHandler
	Sinkers []common.Sinker
	C       *canal.Canal
}

// OnRow 对于 DUMP, 此处的区别是 Header是否为空, 可以判断如果header为空用 insert ignore into, 否则用replace into
func (h *MySQLBinlogHandler) OnRow(e *canal.RowsEvent) error {
	for _, sinker := range h.Sinkers {
		if !sinker.Enable() {
			continue
		}
		err := sinker.OnEvent(e)
		if err != nil && !sinker.ContinueOnError() {
			log.Errorf("On Row, sinker error: " + err.Error())
			sinker.Disable()
		}
	}
	return nil
}

func (h *MySQLBinlogHandler) savePos() {
	atomic.AddUint64(&eventCount, 1)
	if atomic.LoadUint64(&eventCount)%10000 == 0 || time.Now().Unix()-atomic.LoadInt64(&lastNanos) > 5 {
		go func() {
			atomic.StoreInt64(&lastNanos, time.Now().Unix())
			// TODO 定期在 接收事件的时候保存位点
		}()
	}
}

func (h *MySQLBinlogHandler) String() string {
	return "MySQLBinlogHandler"
}

func (h *MySQLBinlogHandler) OnTableChanged(schema string, table string) error {
	log.Infof("OnTableChanged  table %s.%s has changed\n", schema, table)
	return nil
}

func (h *MySQLBinlogHandler) OnRotate(e *replication.RotateEvent) error {
	log.Infof("OnRotate log rotate next log: %s\n", string(e.NextLogName))
	return nil
}
func (h *MySQLBinlogHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	log.Infof("OnDDL pos:%v, query:%s\n", nextPos, string(queryEvent.Query))
	return nil
}
