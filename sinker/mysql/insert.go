package mysql

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/gridsx/datagos/canal/mysql/mapper"
	"github.com/siddontang/go-log/log"
)

// 这个部分可以缓存，不用每次都generate一次， 条件是， mapping 是同一个， 如果 e.Table.name 相同，则直接取 preparedStmt
func (c *MySQLConsumer) toInsert(e *canal.RowsEvent) (string, []int) {
	// 判断是否 mapping 存在， 如果不存在，则认为是对等导入
	defaultPos := make([]int, 0, 8)
	for i := range e.Table.Columns {
		defaultPos = append(defaultPos, i)
	}
	// replace into 等价于delete + insert， 主键修改多一个delete， 会死锁，因此这么写
	isInsert := c.isPrimaryUpdate(e) || e.Action == canal.InsertAction
	if c.Mapping == nil {
		return generateInsertBySrcTable(e.Table.Name, e, isInsert), defaultPos
	} else {
		// 判断源表是否相同
		if e.Table.Name != c.Mapping.SrcTable {
			return "", nil
		}
		// 如果没有配置mapping
		if c.Mapping.ColMappings == nil {
			return generateInsertBySrcTable(c.Mapping.DstTable, e, isInsert), defaultPos
		}
	}
	// 如果配置了mapping, 那么就要配置完整的mapping, 不能只配置一部分
	return generateInsertByMappings(c.Mapping, e, isInsert)
}

// 根据原表直接生成SQL
func generateInsertByMappings(mapping *mapper.TableMapping, e *canal.RowsEvent, isInsert bool) (string, []int) {
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
	if isInsert {
		sqlType = "INSERT IGNORE INTO"
		if e.Action == canal.InsertAction {
			eventNum = len(e.Rows)
		}
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
func generateInsertBySrcTable(dst string, e *canal.RowsEvent, isInsert bool) string {
	colStr := strings.Builder{}
	sqlType := "REPLACE INTO"
	eventNum := len(e.Rows) / 2
	if isInsert {
		sqlType = "INSERT IGNORE INTO"
		if e.Action == canal.InsertAction {
			eventNum = len(e.Rows)
		}
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
