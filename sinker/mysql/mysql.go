package mysql

import "github.com/gridsx/datagos/common"

// binlog过来是顺序的
// sinker 如果保证同一主键顺序写入，不同主键的数据可以并发写入
// TODO 实现Sinker并发写入

import (
	"database/sql"
	"encoding/json"
	"sync"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/gridsx/datagos/canal/mysql/filter"
	"github.com/gridsx/datagos/canal/mysql/mapper"
	"github.com/siddontang/go-log/log"
)

// MySQLSinkerConfig binlog 消费者配置, 一个生产者，可以对应多个消费者
type MySQLSinkerConfig struct {
	DestDatasource common.MySQLInstance  `json:"destDatasource"`
	Filters        []filter.MySQLFilter  `json:"filters"`
	Mappings       []mapper.TableMapping `json:"mappings"`
	ErrorContinue  bool                  `json:"errorContinue"`
}

type MySQLSinker struct {
	disabled      bool
	ErrorContinue bool                 `json:"errorContinue"`
	Filters       []filter.MySQLFilter `json:"filters"`
	Consumers     []*MySQLConsumer     `json:"consumers"`
}

func (s *MySQLSinker) Enable() bool {
	return !s.disabled
}

func (s *MySQLSinker) Disable() {
	s.disabled = true
}

// 过滤器逻辑
func (s *MySQLSinker) filtered(e *canal.RowsEvent) bool {
	if s.Filters != nil {
		for _, v := range s.Filters {
			if v.Match(e) {
				return true
			}
		}
	}
	return false
}

// 事件处理逻辑
func (s *MySQLSinker) OnEvent(e *canal.RowsEvent) error {
	if s.filtered(e) {
		return nil
	}
	// 写入MySQL
	wg := sync.WaitGroup{}
	for _, v := range s.Consumers {
		go func() {
			wg.Add(1)
			err := v.Accept(e)
			if err != nil {
				log.Errorf("event execute error: %s\n", err.Error())
			}
			wg.Done()
		}()
		wg.Wait()
	}
	return nil
}

func (s *MySQLSinker) ContinueOnError() bool {
	return s.ErrorContinue
}

// MySQLConsumer 目前需要自己手动建表， 意味着 Mapping 不能为空
type MySQLConsumer struct {
	DB      *sql.DB
	Mapping *mapper.TableMapping
	Lock    sync.Mutex
}

func (c *MySQLConsumer) Name() string {
	return "MySQLConsumer"
}

func (c *MySQLConsumer) Accept(e *canal.RowsEvent) error {
	if e == nil || e.Table == nil || e.Table.Name != c.Mapping.SrcTable {
		// 如果不是此处理器需要处理的事情，则不处理
		return nil
	}
	return c.exec(e)
}

// 执行落库操作
func (c *MySQLConsumer) exec(e *canal.RowsEvent) error {
	var resultSql string
	var deleteSql string
	var pos []int
	var delPos []int
	switch e.Action {
	case canal.UpdateAction:
		resultSql, pos = c.toInsert(e)
		if c.isPrimaryUpdate(e) {
			deleteSql, delPos = c.toDelete(e, true)
		}
	case canal.InsertAction:
		resultSql, pos = c.toInsert(e)
	case canal.DeleteAction:
		resultSql, pos = c.toDelete(e, false)
	default:
	}
	values, deleteValues := c.prepareValues(e, pos, delPos)

	// update主键的时候删除更新前的行
	if len(deleteSql) > 0 {
		_, delErr := c.DB.Exec(deleteSql, deleteValues...)
		if delErr != nil {
			log.Errorf("error executing delete sql: %s, args: %v err:%s\n", deleteSql, deleteValues, delErr.Error())
			return delErr
		}
	}

	// 正常执行
	_, err := c.DB.Exec(resultSql, values...)
	if err != nil {
		log.Errorf("error executing sql: %s, args: %v, err:%s\n", resultSql, values, err.Error())
		return err
	}
	return nil
}

func (c *MySQLConsumer) prepareValues(e *canal.RowsEvent, valuePos []int, delPos []int) ([]interface{}, []interface{}) {
	// 准备值
	values := make([]interface{}, 0, 8)
	deleteValues := make([]interface{}, 0, 8)
	updatePrimary := c.isPrimaryUpdate(e)
	if canal.DeleteAction == e.Action {
		// 构造 delete 的value列表
		for i := range e.Rows {
			for _, v := range e.Table.PKColumns {
				values = append(values, e.Rows[i][v])
			}
		}

	} else if e.Action == canal.UpdateAction {
		// update的话，偶数行是
		rowNum := len(e.Rows)
		for i := 0; i < rowNum; i++ {
			valueRow := e.Rows[i]
			if i%2 == 0 {
				//before
				if !updatePrimary {
					continue
				}
				for _, v := range delPos {
					deleteValues = append(deleteValues, valueRow[v])
				}
			} else {
				//after
				for _, v := range valuePos {
					values = append(values, valueRow[v])
				}
			}
		}
	} else {
		// 构造 insert value列表
		for i := range e.Rows {
			for _, v := range valuePos {
				values = append(values, e.Rows[i][v])
			}
		}
	}
	// 此处不支持值映射了，return c.processMappings(e, valuePos, values)
	return values, deleteValues
}

func mapTo(current interface{}, s ...interface{}) interface{} {
	if len(s) == 0 || len(s)%2 != 0 {
		return current
	}
	for i := 0; i+1 < len(s); i += 2 {
		if s[i] == current {
			return s[i+1]
		}
	}
	return current
}

func (c *MySQLConsumer) isPrimaryUpdate(e *canal.RowsEvent) bool {
	if e.Action != canal.UpdateAction {
		return false
	}
	if len(e.Rows) != 2 {
		return false
	}
	for _, colPos := range e.Table.PKColumns {
		if e.Rows[0][colPos] != e.Rows[1][colPos] {
			return true
		}
	}
	return false
}

func Build(c string) *MySQLSinker {
	cfg := new(MySQLSinkerConfig)
	_ = json.Unmarshal([]byte(c), cfg)
	filters := cfg.Filters
	consumers := make([]*MySQLConsumer, 0, 4)
	instDB := cfg.DestDatasource.ToDatasource()
	for _, m := range cfg.Mappings {
		consumers = append(consumers, &MySQLConsumer{
			DB:      instDB,
			Mapping: &m,
			Lock:    sync.Mutex{},
		})
	}
	return &MySQLSinker{
		ErrorContinue: cfg.ErrorContinue,
		Filters:       filters,
		Consumers:     consumers,
	}
}
