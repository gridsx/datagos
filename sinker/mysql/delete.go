package mysql

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/canal"
)

// 取出主键，取出对应的值，生成Delete语句即可
func (c *MySQLConsumer) toDelete(e *canal.RowsEvent, isUpdate bool) (string, []int) {
	if c.Mapping == nil {
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
		return prefix + condHeader.String() + buildDeletePrimaryWhere(e, isUpdate), e.Table.PKColumns
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
		return prefix + condHeader.String() + buildDeletePrimaryWhere(e, isUpdate), e.Table.PKColumns
	}
}

func getDstName(c *MySQLConsumer, colName string) string {
	for _, v := range c.Mapping.ColMappings {
		if colName == v.Src {
			return v.Dst
		}
	}
	return colName
}

// 拼值 如： ((1, 2), (413, 2))
func buildDeletePrimaryWhere(e *canal.RowsEvent, isUpdate bool) string {
	where := strings.Builder{}
	where.WriteString("(")
	rowNum := len(e.Rows)
	if isUpdate {
		rowNum = 1
	}
	for i := 0; i < rowNum; i++ {
		where.WriteString("(")
		for p := range e.Table.PKColumns {
			where.WriteString("?")
			if p != len(e.Table.PKColumns)-1 {
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
