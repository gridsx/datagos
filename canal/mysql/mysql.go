package mysql

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gridsx/datagos/canal/common"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/gridsx/datagos/canal/mysql/meta"
	"github.com/gridsx/datagos/canal/mysql/sinker"
	"github.com/gridsx/datagos/task"
	"github.com/siddontang/go-log/log"
)

const dumpExec = "mysqldump"

// binlog 位置5秒保存一次
const binlogPosSaveDuration int64 = 5

type CanalTask struct {
	c          *canal.Canal
	dumpFinish chan bool
	dump       bool
	dumpPos    mysql.Position
	key        string
	running    bool
	Inst       *meta.MySQLSrcConfig
	seconds    int64
	lock       sync.Mutex
	mgr        *task.Task
}

func (t *CanalTask) onDumpFinish() {
	//  TODO 记录dump是否完成， 如果已经完成， 那么下次开始的时候，则不需要dump
}

// Start 开始任务, 如果任务中包含全量， 则新起slave 监听
// 待dump 完成， 同时停掉已有任务，以 修改原来连接上的任务， 比较 dump 与 之前binlog监听的位点，
// 取位点较小的做为 下次启动开始消费的点
func (t *CanalTask) Start() error {
	if t.running {
		log.Warnf("canal task start, already running, task: %s\n", t.Inst.MySQLInstance.Host)
		return nil
	}
	err := t.mgr.UpdateTaskState(task.Running)

	if err != nil {
		return err
	}
	t.running = true
	t.updateBinlog()

	// dump 数据， 如果存在全量配置，那么就先全量，后增量
	if t.dump {
		dumpErr := t.c.Dump()
		if dumpErr != nil {
			t.Stop()
			return dumpErr
		}
		t.onDumpFinish()
	}

	// 如果任务本身有position， 拿任务本身的position， 没有的话拿当前binlog点位
	var pos *mysql.Position
	if t.Inst.Position != nil {
		pos = t.Inst.Position
	} else {
		if t.dump {
			syncedPos := t.c.SyncedPosition()
			pos = &syncedPos
		} else {
			if pos == nil {
				masterPos, posErr := t.c.GetMasterPos()
				if posErr == nil {
					pos = &masterPos
				}
			}
		}
	}

	if pos == nil {
		return errors.New("error getting position")
	}
	runErr := t.c.RunFrom(*pos)

	if runErr != nil {
		t.Stop()
		return runErr
	}
	return nil
}

//  定时任务更新 slave的binlog同步到什么地方的位点信息
func (t *CanalTask) updateBinlog() {
	go func() {
		for {
			if t.running {
				time.Sleep(time.Second)
				nowSecond := time.Now().Unix()
				lastSecond := atomic.LoadInt64(&t.seconds)
				if nowSecond-lastSecond > binlogPosSaveDuration {
					atomic.StoreInt64(&t.seconds, nowSecond)
					t.updateTaskBinlog()
				}
			} else {
				break
			}
		}
	}()
}

// Stop 停止任务， 停止canal的消费，取消slave监听， 停掉的时候需要记录 binlog  position
func (t *CanalTask) Stop() {
	if !t.running {
		log.Warnf("canal task stop already stopped, task id : %d\n", t.mgr.Id)
		return
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	t.running = false
	t.updateTaskBinlog()
	uerr := t.mgr.UpdateTaskState(task.Stopped)
	if uerr != nil {
		log.Errorf("error updating instance state: %v\n", uerr)
	}
	t.c.Close()
}

func (t *CanalTask) updateTaskBinlog() {
	infoMap := make(map[string]interface{}, 1)
	infoMap["position"] = t.c.SyncedPosition()
	d, _ := json.Marshal(infoMap)
	err := t.mgr.UpdateTaskInfo(string(d))
	if err != nil {
		log.Errorf("error updating instance position: %v\n", err)
	}
}

func (t *CanalTask) Running() bool {
	return t.running
}

func (t *CanalTask) GetDelay() uint32 {
	return t.c.GetDelay()
}

func NewMySQLCanalTask(t *task.Task) *CanalTask {
	if t.SrcType != int(task.SrcMySQL) {
		return nil
	}
	sinkers := builderSinkers(t)
	if sinkers == nil {
		return nil
	}
	return newCanalTask(t, sinkers)
}

func builderSinkers(t *task.Task) []common.Sinker {
	dest, err := t.GetDest()
	if err != nil {
		return nil
	}
	sinkers := make([]common.Sinker, 0, len(dest))
	for _, d := range dest {
		cfg := new(meta.MySQLSinkerConfig)
		_ = json.Unmarshal([]byte(d.Config), cfg)
		filters := cfg.Filters
		consumers := make([]*sinker.MySQLConsumer, 0, 4)
		instDB := cfg.DestDatasource.ToDatasource()
		for _, m := range cfg.Mappings {
			consumers = append(consumers, &sinker.MySQLConsumer{
				DB:      instDB,
				Mapping: &m,
				Lock:    sync.Mutex{},
			})
		}
		sinkers = append(sinkers, &sinker.MySQLSinker{
			ErrorContinue: cfg.ErrorContinue,
			Filters:       filters,
			Consumers:     consumers,
		})
	}
	return sinkers
}

func newCanalTask(t *task.Task, sinkers []common.Sinker) *CanalTask {
	s := new(meta.MySQLSrcConfig)
	err := json.Unmarshal([]byte(t.Src), s)
	if err != nil {
		return nil
	}

	c := s.ToServerConfig()
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", c.MasterInfo.Host, c.MasterInfo.Port)
	cfg.User = c.MasterInfo.Username
	cfg.Password = c.MasterInfo.Password
	cfg.HeartbeatPeriod = time.Second * 5
	cfg.DiscardNoMetaRowEvent = true
	cfg.TimestampStringLocation = time.UTC

	// TODO 补充 filter.
	// cfg.ExcludeTableRegex, 是否拉取这个日志
	// cfg.IncludeTableRegex， 是否拉取

	if c.DumpFilter != nil {
		// 全量配置相关
		cfg.Dump.ExecutionPath = `mysqldump`
		cfg.Dump.SkipMasterData = true
		cfg.Dump.Databases = c.DumpFilter.Databases
		cfg.Dump.Where = c.DumpFilter.Where
		cfg.Dump.TableDB = c.DumpFilter.TableDB
		cfg.Dump.Tables = c.DumpFilter.Tables
		cfg.Dump.IgnoreTables = c.DumpFilter.IgnoreTables
	} else {
		cfg.Dump.ExecutionPath = ""
	}

	cx, err := canal.NewCanal(cfg)
	if err != nil {
		log.Errorln(err)
		return nil
	}
	cx.SetEventHandler(&sinker.MySQLBinlogHandler{Inst: s, Sinkers: sinkers, C: cx})
	return &CanalTask{
		c:          cx,
		Inst:       s,
		dumpFinish: make(chan bool),
		dump:       c.DumpFilter != nil, // TODO dump finished
		key:        fmt.Sprintf("%d", t.Id),
		lock:       sync.Mutex{},
		mgr:        t,
	}
}