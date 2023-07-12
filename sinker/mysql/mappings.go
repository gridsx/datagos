package mysql

import (
	"github.com/antonmedv/expr"
	"github.com/go-mysql-org/go-mysql/canal"
)

// 这里是一些比较高级的用法，用于将原表的值，经过表达式计算，再落到目标表里面
// 此类计算会比较消耗CPU资源， 因此不推荐使用

func (c *MySQLConsumer) processMappings(e *canal.RowsEvent, valuePos []int, values []interface{}) []interface{} {
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
