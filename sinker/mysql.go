package sinker

import (
	"database/sql"
	"fmt"
	"github.com/antonmedv/expr"
	"strings"
	"sync"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/gridsx/datagos/filter"
	"github.com/gridsx/datagos/mapper"
	"github.com/siddontang/go-log/log"
)

type MySQLSinker struct {
	disabled      bool
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
		go func() {
			err := v.Accept(e)
			if err != nil {
				log.Errorf("event execute error: %s\n", err.Error())
			}
		}()
	}
	return nil
}

func (s *MySQLSinker) continueOnError() bool {
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

// 执行落库
// TODO 主键修改尚未完成，应该解析成一条插入一条删除， 此场景比较少
func (c *MySQLConsumer) exec(e *canal.RowsEvent) error {
	var resultSql string
	var pos []int
	switch e.Action {
	case canal.UpdateAction, canal.InsertAction:
		resultSql, pos = c.toInsert(e)
	case canal.DeleteAction:
		resultSql, pos = c.toDelete(e)
	default:
	}
	values := c.prepareValues(e, pos)
	_, err := c.DB.Exec(resultSql, values...)
	if err != nil {
		log.Errorf("error executing sql: %s, err:%s\n", resultSql, err.Error())
		return err
	}
	return nil
}

func (c *MySQLConsumer) prepareValues(e *canal.RowsEvent, valuePos []int) []interface{} {
	// 准备值
	values := make([]interface{}, 0, 8)
	if canal.DeleteAction == e.Action {
		// 构造 delete 的value列表
		for i := range e.Rows {
			for _, v := range e.Table.PKColumns {
				values = append(values, e.Rows[i][v])
			}
		}

	} else {
		// 构造 insert 和update 的 value列表
		if e.Action == canal.UpdateAction {
			// update的话，偶数行是
			rowNum := len(e.Rows)
			for i := 0; i < rowNum; i += 2 {
				if i+1 >= rowNum {
					break
				}
				valueRow := e.Rows[i+1]
				for _, v := range valuePos {
					values = append(values, valueRow[v])
				}
			}
		} else {
			for i := range e.Rows {
				for _, v := range valuePos {
					values = append(values, e.Rows[i][v])
				}
			}
		}
	}

	// 字段映射值处理逻辑， 用于处理值映射问题
	if c.Mapping != nil && c.Mapping.ColMappings != nil {
		rowLen := len(e.Rows)
		if e.Action == canal.UpdateAction {
			for r := 0; r < rowLen; r += 2 {
				if r+1 >= rowLen {
					break
				}
				for i, cm := range c.Mapping.ColMappings {
					if len(cm.Expr) > 0 {
						envMap := make(map[string]interface{}, 8)
						envMap["last"] = e.Rows[r][valuePos[i]]
						envMap["current"] = e.Rows[r+1][valuePos[i]]
						envMap["mapTo"] = mapTo
						program, err := expr.Compile(cm.Expr, expr.Env(envMap))
						output, err := expr.Run(program, envMap)
						if err == nil {
							values[valuePos[i]] = output
						}
					}
				}
			}
		} else if e.Action == canal.DeleteAction {
			for r := 0; r < rowLen; r++ {
				for i, cm := range c.Mapping.ColMappings {
					if len(cm.Expr) > 0 {
						envMap := make(map[string]interface{}, 8)
						// 对于delete对应的 value的值的位置不等， 因此要取的值比较麻烦一些
						// 找出SRC 字段, 如果不是主键忽略， 如果是主键字段，则需要根据SRC找到原字段所在位置
						for _, v := range e.Table.PKColumns {
							if cm.Src == e.Table.Columns[v].Name {
								// 如果映射的是主键， 那么需要对应映射做好
								envMap["current"] = e.Rows[i][v]
								break
							}
						}
						envMap["mapTo"] = mapTo
						program, err := expr.Compile(cm.Expr, expr.Env(envMap))
						output, err := expr.Run(program, envMap)
						if err == nil && e.Action == canal.DeleteAction {
							for kp, v := range e.Table.PKColumns {
								if cm.Src == e.Table.Columns[v].Name {
									// 如果映射的是主键， 那么需要对应映射做好
									values[kp] = output
									break
								}
							}
						}
					}
				}
			}
		} else if e.Action == canal.InsertAction {
			for r := 0; r < rowLen; r++ {
				for i, cm := range c.Mapping.ColMappings {
					if len(cm.Expr) > 0 {
						envMap := make(map[string]interface{}, 8)
						envMap["current"] = e.Rows[r][valuePos[i]]
						envMap["mapTo"] = mapTo
						program, err := expr.Compile(cm.Expr, expr.Env(envMap))
						output, err := expr.Run(program, envMap)
						if err == nil {
							values[valuePos[i]] = output
						}
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
		prefix := fmt.Sprintf("DELETE FROM `%s` WHERE ", e.Table.Name)

		// 拼头部如 (id, type) IN
		condHeader := strings.Builder{}
		condHeader.WriteString("(")
		for i, v := range e.Table.PKColumns {
			condHeader.WriteString(fmt.Sprintf("`%s`", e.Table.Columns[v].Name))
			if i != len(e.Table.PKColumns)-1 {
				condHeader.WriteString(", ")
			}
		}
		condHeader.WriteString(") IN ")
		// 拼写结果如： DELETE FROM demo where (id, one) in ((1, 2), (413, 2))
		return prefix + condHeader.String() + buildDeletePrimaryWhere(e), e.Table.PKColumns
	} else {
		prefix := fmt.Sprintf("DELETE FROM `%s` WHERE ", c.Mapping.DstTable)

		// 构建头部，映射后的：
		condHeader := strings.Builder{}
		condHeader.WriteString("(")
		for i, v := range e.Table.PKColumns {
			// 只用映射的字段, 这里或许可以优化
			colName := e.Table.Columns[v].Name
			dstName := getDstName(c, colName)
			condHeader.WriteString(fmt.Sprintf("`%s`", dstName))
			if i < len(e.Table.PKColumns)-1 {
				condHeader.WriteString(", ")
			}
		}
		condHeader.WriteString(") IN ")
		return prefix + condHeader.String() + buildDeletePrimaryWhere(e), e.Table.PKColumns
	}
}

func getDstName(c *MySQLConsumer, colName string) string {
	for _, v := range c.Mapping.ColMappings {
		if colName == v.Src {
			return v.Dst
		}
	}
	return ""
}

// 拼值 如： ((1, 2), (413, 2))
func buildDeletePrimaryWhere(e *canal.RowsEvent) string {
	where := strings.Builder{}
	where.WriteString("(")
	rowNum := len(e.Rows)
	for i := 0; i < rowNum; i++ {
		where.WriteString("(")
		for p := range e.Table.PKColumns {
			where.WriteString("?")
			if p != len(e.Table.Columns)-1 {
				where.WriteString(", ")
			}
		}
		where.WriteString(")")
		if i != rowNum-1 {
			where.WriteString(", ")
		}
	}
	where.WriteString(");")
	return where.String()
}

// 根据原表直接生成SQL
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
		// 配置有误， mapping 里面存在binlog中不包含的列，无法处理， 如有后续需求，可额外扩展
		log.Errorf("mappings may not correct: %v\n", mapping.ColMappings)
		return "", nil
	}

	// 拼头部信息
	colStr := strings.Builder{}
	eventNum := len(e.Rows) / 2
	sqlType := "REPLACE INTO"
	if e.Action == canal.InsertAction {
		sqlType = "INSERT IGNORE INTO"
		eventNum = len(e.Rows)
	}
	colStr.WriteString(fmt.Sprintf(sqlType+" `%s`", mapping.DstTable))
	colStr.WriteString("(")
	for i, v := range mapping.ColMappings {
		colStr.WriteString(fmt.Sprintf("`%s`", v.Dst))
		if i != len(mapping.ColMappings)-1 {
			colStr.WriteString(", ")
		}
	}
	colStr.WriteString(") VALUES")

	// 构建值
	valueStr := strings.Builder{}
	for i := 0; i < eventNum; i++ {
		valueStr.WriteString("(")
		for i, _ := range mapping.ColMappings {
			valueStr.WriteString("?")
			if i != len(mapping.ColMappings)-1 {
				valueStr.WriteString(", ")
			}
		}
		valueStr.WriteString(")")
		if i == eventNum-1 {
			valueStr.WriteString(";")
		} else {
			valueStr.WriteString(", ")
		}
	}
	return colStr.String() + valueStr.String(), locations
}

// 根据映射直接生成SQL
func generateBySrcTable(dst string, e *canal.RowsEvent) string {
	colStr := strings.Builder{}
	sqlType := "REPLACE INTO"
	eventNum := len(e.Rows) / 2
	if e.Action == canal.InsertAction {
		sqlType = "INSERT IGNORE INTO"
		eventNum = len(e.Rows)
	}
	colStr.WriteString(fmt.Sprintf(sqlType+" `%s`", dst))
	colStr.WriteString("(")

	for i, v := range e.Table.Columns {
		colStr.WriteString(fmt.Sprintf("`%s`", v.Name))
		if i != len(e.Table.Columns)-1 {
			colStr.WriteString(", ")
		}
	}
	colStr.WriteString(") VALUES")

	// 	构建值
	valueStr := strings.Builder{}
	for i := 0; i < eventNum; i++ {
		valueStr.WriteString("(")
		for i := range e.Table.Columns {
			valueStr.WriteString("?")
			if i != len(e.Table.Columns)-1 {
				valueStr.WriteString(", ")
			}
		}
		valueStr.WriteString(")")
		if i == eventNum-1 {
			valueStr.WriteString(";")
		} else {
			valueStr.WriteString(", ")
		}
	}
	return colStr.String() + valueStr.String()
}
