package sinker

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/antonmedv/expr"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/gridsx/datagos/filter"
	"github.com/gridsx/datagos/mapper"
	"github.com/siddontang/go-log/log"
)

type MySQLSinker struct {
	disabled      bool             `json:"-"`
	ErrorContinue bool             `json:"errorContinue"`
	Filters       []filter.Filter  `json:"filters"`
	Consumers     []*MySQLConsumer `json:"consumers"`
}

func (s *MySQLSinker) enable() bool {
	return !s.disabled
}

func (s *MySQLSinker) disable() {
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
func (s *MySQLSinker) onEvent(e *canal.RowsEvent) error {
	if s.filtered(e) {
		return nil
	}
	// 写入MySQL
	for _, v := range s.Consumers {
		// TODO 此处需要异步处理，一个携程处理一个consumer
		go v.Accept(e)
	}
	return nil
}

func (s *MySQLSinker) continueOnError() bool {
	return s.ErrorContinue
}

// MySQLConsumer 目前需要自己手动建表， 意味着 Mapping 不能为空
type MySQLConsumer struct {
	DB          *sql.DB
	Mapping     *mapper.TableMapping
	InsertStmt  *sql.Stmt
	ReplaceStmt *sql.Stmt
	DeleteStmt  *sql.Stmt
	ValuePos    []int // 适用于 insert/update 不适用于delete
	Lock        sync.Mutex
}

func (c *MySQLConsumer) Name() string {
	return "MySQLConsumer"
}

func (c *MySQLConsumer) Accept(e *canal.RowsEvent) error {
	if e == nil || e.Table.Schema != e.Table.Schema || e.Table.Name != c.Mapping.SrcTable {
		// 如果不是此处理器需要处理的事情，则不处理
		return nil
	}
	if err := c.prepare(e); err != nil {
		return err
	}
	return c.exec(e)
}

// 执行落库
// TODO 主键修改尚未完成，应该解析成一条插入一条删除， 此场景比较少
// TODO 看binlog来的姿势来看， update 和 insert 很有可能是批量来的， 主要是为了提高速率
// preparedStmt 会执行的快一些， 因此要使用preparedStmt, 但是，由于

func (c *MySQLConsumer) exec(e *canal.RowsEvent) error {
	var stmt *sql.Stmt
	switch e.Action {
	case canal.InsertAction:
		stmt = c.InsertStmt
	case canal.UpdateAction:
		stmt = c.ReplaceStmt
	case canal.DeleteAction:
		stmt = c.DeleteStmt
	default:
		return nil
	}
	values := c.prepareValues(e)
	_, err := stmt.Exec(values...)
	if err != nil {
		log.Errorf("error executing insert sql, sql: %s, error: %s", stmt, err.Error())
		return err
	}
	return nil
}

func (c *MySQLConsumer) prepareValues(e *canal.RowsEvent) []interface{} {
	// 准备值
	values := make([]interface{}, 0, 8)
	if canal.DeleteAction == e.Action {
		for _, v := range e.Table.PKColumns {
			values = append(values, e.Rows[0][v])
		}
	} else {
		for _, v := range c.ValuePos {
			if e.Action == canal.UpdateAction {
				values = append(values, e.Rows[1][v])
			} else if e.Action == canal.InsertAction {
				values = append(values, e.Rows[0][v])
			}
		}
	}

	// 字段映射值处理逻辑， 用于处理值映射问题
	if c.Mapping != nil && c.Mapping.ColMappings != nil {
		for i, cm := range c.Mapping.ColMappings {
			if len(cm.Expr) > 0 {
				envMap := make(map[string]interface{}, 8)
				if e.Action == canal.UpdateAction {
					envMap["last"] = e.Rows[0][c.ValuePos[i]]
					envMap["current"] = e.Rows[1][c.ValuePos[i]]
				} else if e.Action == canal.InsertAction {
					envMap["current"] = e.Rows[0][c.ValuePos[i]]
				} else if e.Action == canal.DeleteAction {
					// 对于delete对应的 value的值的位置不等， 因此要取的值比较麻烦一些
					// 找出SRC 字段, 如果不是主键忽略， 如果是主键字段，则需要根据SRC找到原字段所在位置
					for _, v := range e.Table.PKColumns {
						if cm.Src == e.Table.Columns[v].Name {
							// 如果映射的是主键， 那么需要对应映射做好
							envMap["current"] = e.Rows[0][v]
							break
						}
					}
				}
				envMap["mapTo"] = mapTo
				program, err := expr.Compile(cm.Expr, expr.Env(envMap))
				output, err := expr.Run(program, envMap)
				if err == nil {
					if e.Action == canal.DeleteAction {
						for kp, v := range e.Table.PKColumns {
							if cm.Src == e.Table.Columns[v].Name {
								// 如果映射的是主键， 那么需要对应映射做好
								values[kp] = output
								break
							}
						}
					} else {
						values[c.ValuePos[i]] = output
					}
				}
			}
		}
	}
	return values
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

func (c *MySQLConsumer) prepare(e *canal.RowsEvent) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	sqlStr := ""
	var args []int
	switch e.Action {
	case canal.InsertAction:
		if c.InsertStmt != nil {
			return nil
		}
		sqlStr, args = c.toInsert(e)
	case canal.UpdateAction:
		if c.ReplaceStmt != nil {
			return nil
		}
		sqlStr, args = c.toInsert(e)
	case canal.DeleteAction:
		if c.DeleteStmt != nil {
			return nil
		}
		sqlStr, args = c.toDelete(e)
	default:
		return nil
	}
	if len(sqlStr) == 0 {
		return errors.New("error generating sql statement")
	}
	stmt, err := c.DB.Prepare(sqlStr)
	if err != nil {
		log.Errorf("Accept prepare stmt,  sql:%s error: %s\n", sqlStr, err.Error())
		return err
	}
	switch e.Action {
	case canal.InsertAction:
		c.InsertStmt = stmt
	case canal.UpdateAction:
		c.ReplaceStmt = stmt
	case canal.DeleteAction:
		c.DeleteStmt = stmt
	default:
		return nil
	}
	if e.Action != canal.DeleteAction {
		c.ValuePos = args
	}
	return nil
}

// 这个部分可以缓存，不用每次都generate一次， 条件是， mapping 是同一个， 如果 e.Table.name 相同，则直接取 preparedStmt
func (c *MySQLConsumer) toInsert(e *canal.RowsEvent) (string, []int) {
	// 判断是否 mapping 存在， 如果不存在，则认为是对等导入
	defaultPos := make([]int, 0, 8)
	for i := range e.Table.Columns {
		defaultPos = append(defaultPos, i)
	}

	if c.Mapping == nil {
		return generateBySrcTable(e.Table.Name, e), defaultPos
	} else {
		// 判断源表是否相同
		if e.Table.Name != c.Mapping.SrcTable {
			return "", nil
		}
		// 如果没有配置mapping
		if c.Mapping.ColMappings == nil {
			return generateBySrcTable(c.Mapping.DstTable, e), defaultPos
		}
	}
	// 如果配置了mapping, 那么就要配置完整的mapping, 不能只配置一部分
	return generateByMappings(c.Mapping, e)
}

// 取出主键，取出对应的值，生成Delete语句即可
func (c *MySQLConsumer) toDelete(e *canal.RowsEvent) (string, []int) {
	if c.Mapping == nil || c.Mapping.ColMappings == nil {
		rawSql := fmt.Sprintf("DELETE FROM `%s` WHERE ", e.Table.Name)
		where := ""
		for i, v := range e.Table.PKColumns {
			colName := e.Table.Columns[v].Name
			where += fmt.Sprintf("`%s` = ? ", colName)
			if i < len(e.Table.PKColumns)-1 {
				where += " AND "
			}
		}
		return rawSql + where, e.Table.PKColumns
	} else {
		rawSql := fmt.Sprintf("DELETE FROM `%s` WHERE ", c.Mapping.DstTable)
		where := ""
		for i, v := range e.Table.PKColumns {
			// 只用映射的字段, 这里或许可以优化
			colName := e.Table.Columns[v].Name
			for _, v := range c.Mapping.ColMappings {
				if colName == v.Src {
					where += fmt.Sprintf("`%s` = ? ", v.Dst)
					break
				}
			}
			if i < len(e.Table.PKColumns)-1 {
				where += " AND "
			}
		}
		return rawSql + where, e.Table.PKColumns
	}
	//
	//if c.Mapping.ColMappings
	//return "DELETE FROM " +
	//	positions := e.Table.PKColumns

	return "", nil
}

func generateByMappings(mapping *mapper.TableMapping, e *canal.RowsEvent) (string, []int) {
	locations := make([]int, 0, 8)

	for _, m := range mapping.ColMappings {
		for j, c := range e.Table.Columns {
			// 如果对的上列, 那么就把这个列的位置记录下来
			if m.Src == c.Name {
				locations = append(locations, j)
			}
		}
	}

	if len(locations) != len(mapping.ColMappings) {
		// 配置有误， mapping 里面存在binlog中不包含的列，无法处理
		// 如有后续需求，可额外扩展
		return "", nil
	}

	colStr, valueStr := strings.Builder{}, strings.Builder{}
	sqlType := "REPLACE INTO"
	if e.Header == nil {
		sqlType = "INSERT IGNORE INTO"
	}
	colStr.WriteString(fmt.Sprintf(sqlType+" `%s`", mapping.DstTable))
	valueStr.WriteString("VALUES")
	colStr.WriteString("(")
	valueStr.WriteString("(")
	for i, v := range mapping.ColMappings {
		colStr.WriteString(fmt.Sprintf("`%s`", v.Dst))
		valueStr.WriteString("?")
		if i != len(mapping.ColMappings)-1 {
			colStr.WriteString(", ")
			valueStr.WriteString(", ")
		}
	}
	colStr.WriteString(")")
	valueStr.WriteString(");")
	return colStr.String() + valueStr.String(), locations
}

func generateBySrcTable(dst string, e *canal.RowsEvent) string {
	colStr, valueStr := strings.Builder{}, strings.Builder{}
	sqlType := "REPLACE INTO"
	if e.Header == nil {
		sqlType = "INSERT IGNORE INTO"
	}
	colStr.WriteString(fmt.Sprintf(sqlType+" `%s`", dst))
	valueStr.WriteString("VALUES")
	colStr.WriteString("(")
	valueStr.WriteString("(")
	for i, v := range e.Table.Columns {
		colStr.WriteString(fmt.Sprintf("`%s`", v.Name))
		valueStr.WriteString("?")
		if i != len(e.Table.Columns)-1 {
			colStr.WriteString(", ")
			valueStr.WriteString(", ")
		}
	}
	colStr.WriteString(")")
	valueStr.WriteString(");")
	return colStr.String() + valueStr.String()
}
