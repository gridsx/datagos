package task

import (
	"errors"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/gridsx/datagos/meta"
	"github.com/gridsx/datagos/sinker"
	"github.com/siddontang/go-log/log"
	"sync"
	"sync/atomic"
	"time"
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
	Inst       *meta.InstanceInfo
	seconds    int64
	lock       sync.Mutex
}

func (t *CanalTask) onDumpFinish() {
	go func() {
		err := meta.Manager.UpdateDumpConfig(t.Inst.Id, "")
		if err != nil {
			log.Errorf("onDumpFinish dump finish update db failed! error : %v\n", err)
		}
	}()
}

// Start 开始任务, 如果任务中包含全量， 则新起slave 监听
// 待dump 完成， 同时停掉已有任务，以 修改原来连接上的任务， 比较 dump 与 之前binlog监听的位点，
// 取位点较小的做为 下次启动开始消费的点
func (t *CanalTask) Start() error {
	if t.running {
		log.Warnf("canal task start, already running, task: %d\n", t.Inst.Id)
		return nil
	}
	t.lock.Lock()
	// 开始的时候就放内存里管理
	if GetTask(fmt.Sprintf("%d", t.Inst.Id)) != nil {
		t.lock.Unlock()
		return nil
	}
	StoreTask(t)
	t.lock.Unlock()
	err := meta.Manager.UpdateInstanceState(t.Inst.Id, meta.InstanceRunning)
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
			RemoveTask(t)
			return dumpErr
		}
		t.onDumpFinish()
	}

	if t.HasRunning() {
		//  TODO 如果有正在跑的， 复杂一些， 需要拿到可以处理对方任务的 canal并停下来， 拿到 它的binlog位点
		// 与已有的进行比较， 当然同一个 slave 的复制任务应该在同一个节点上， 这个要保证
		pos, err := t.c.GetMasterPos()
		if err != nil {
			log.Errorf("error getting master pos after dump, %v", pos)
			return err
		}
		t.dumpPos = pos
		t.Stop()

	} else {
		// 如果任务本身有position， 拿任务本身的position， 没有的话拿当前binlog点位
		var pos *mysql.Position
		if len(t.Inst.Position) > 0 {
			pos = t.Inst.ToPosition()
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
		err := t.c.RunFrom(*pos)

		if err != nil {
			t.Stop()
			return err
		}
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
					err := meta.Manager.SavePosition(t.Inst.Id, t.c.SyncedPosition())
					if err != nil {
						log.Errorf("binlog save task failed, error: %v", err)
					}
				}
			} else {
				break
			}
		}
	}()
}

// HasRunning 判断是否有正在运行的slave
// TODO 实现
// 与 zk 进行交互，拿到所有节点正在运行的任务信息
func (t *CanalTask) HasRunning() bool {
	return false
}

// Stop 停止任务， 停止canal的消费，取消slave监听， 停掉的时候需要记录 binlog  position
func (t *CanalTask) Stop() {
	if !t.running {
		log.Warnf("canal task stop already stopped, task id : %d\n", t.Inst.Id)
		return
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	t.running = false
	RemoveTask(t)
	err := meta.Manager.SavePosition(t.Inst.Id, t.c.SyncedPosition())
	if err != nil {
		log.Errorf("error updating instance position: %v\n", err)
	}
	uerr := meta.Manager.UpdateInstanceState(t.Inst.Id, meta.InstanceStopped)
	if uerr != nil {
		log.Errorf("error updating instance state: %v\n", uerr)
	}
	t.c.Close()
}

func (t *CanalTask) Running() bool {
	return t.running
}

func (t *CanalTask) GetDelay() uint32 {
	return t.c.GetDelay()
}

func NewMySQLCanalTask(s *meta.InstanceInfo, sinkerConfigs []meta.MySQLSinkerConfig) *CanalTask {
	sinkers := make([]sinker.Sinker, 0, 4)
	for _, cfg := range sinkerConfigs {
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
	return NewCanalTask(s, sinkers)
}

func NewCanalTask(s *meta.InstanceInfo, sinkers []sinker.Sinker) *CanalTask {
	c := s.ToServerConfig()
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", c.MasterInfo.Host, c.MasterInfo.Port)
	cfg.User = c.MasterInfo.Username
	cfg.Password = c.MasterInfo.Password
	cfg.HeartbeatPeriod = time.Second * 5
	cfg.DiscardNoMetaRowEvent = true

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
		log.Fatal(err)
		return nil
	}
	cx.SetEventHandler(&sinker.BinlogHandler{Inst: s, Sinkers: sinkers, C: cx})

	return &CanalTask{
		c:          cx,
		Inst:       s,
		dumpFinish: make(chan bool),
		dump:       c.DumpFilter != nil,
		key:        fmt.Sprintf("%d", s.Id),
		lock:       sync.Mutex{},
	}
}
