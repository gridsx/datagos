package sinker

import (
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/gridsx/datagos/meta"
	"github.com/siddontang/go-log/log"
)

type BinlogHandler struct {
	canal.DummyEventHandler
	Sinkers []Sinker
	C       *canal.Canal
	Inst    *meta.InstanceInfo
}

// OnRow 对于 DUMP, 此处的区别是 Header是否为空, 可以判断如果header为空用 insert ignore into, 否则用replace into
func (h *BinlogHandler) OnRow(e *canal.RowsEvent) error {
	// TODO 在sink之后，内存记录位点， 位点定时刷新到 DB中去， 这样如果任务终止，或者任务本身有错误
	// 可以通过重新拾取sink之前成功的位点继续消费， 间接保证数据一致性
	for _, sinker := range h.Sinkers {
		if !sinker.enable() {
			continue
		}
		err := sinker.onEvent(e)
		if err != nil && !sinker.continueOnError() {
			log.Errorf("On Row, sinker error: " + err.Error())
			sinker.disable()
		}
	}
	return nil
}

func (h *BinlogHandler) String() string {
	return "BinlogHandler"
}

func (h *BinlogHandler) OnTableChanged(schema string, table string) error {
	return h.saveBinlogPos()
}

func (h *BinlogHandler) OnRotate(*replication.RotateEvent) error {
	return h.saveBinlogPos()
}
func (h *BinlogHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	return h.saveBinlogPos()
}

// 这些时候也保存一下 binlog 位点, 会不会跟5秒一次的冲突？？
func (h *BinlogHandler) saveBinlogPos() error {
	pos := h.C.SyncedPosition()
	h.C.GetMasterPos()
	return meta.Manager.SavePosition(h.Inst.Id, pos)
}
