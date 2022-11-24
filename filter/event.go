package filter

import (
	"github.com/antonmedv/expr"
	"github.com/go-mysql-org/go-mysql/canal"
)

type EventDataFilter struct {
	Databases []string `json:"databases,omitempty"`
	Tables    []string `json:"tables,omitempty"`

	// 过滤掉的数据 expr
	Exclude string `json:"exclude,omitempty"`

	// 保留的数据
	Include string `json:"include,omitempty"`
}

func (f *EventDataFilter) Match(e *canal.RowsEvent) bool {

	// 如果设置了库表过滤， 那么在库表范围内的，则认为是需要
	if f.preMatch(e) {
		return true
	}

	// 如果没有设置表达式，则不过滤，默认 return false
	if len(f.Exclude) == 0 {
		return false
	}

	env := map[string]interface{}{}
	schemaMap := make(map[string]interface{}, 1)
	colMap := make(map[string]interface{}, len(e.Table.Columns))
	schemaMap[e.Table.Name] = colMap
	env[e.Table.Schema] = schemaMap
	for i, col := range e.Table.Columns {
		if e.Action == canal.UpdateAction {
			colMap[col.Name] = e.Rows[1][i]
		} else {
			colMap[col.Name] = e.Rows[0][i]
		}
	}

	if len(f.Exclude) > 0 {
		program, err := expr.Compile(f.Exclude, expr.Env(env))
		if err != nil {
			return false
		}

		output, err := expr.Run(program, env)
		if err != nil {
			return false
		}
		if v, ok := output.(bool); ok {
			return v
		}
	}

	if len(f.Include) > 0 {
		program, err := expr.Compile(f.Include, expr.Env(env))
		if err != nil {
			return false
		}

		output, err := expr.Run(program, env)
		if err != nil {
			return false
		}
		if v, ok := output.(bool); ok {
			return v
		}
	}
	return false
}

// 检查库表是否在范围内，如果不在，则是true
func (f *EventDataFilter) preMatch(e *canal.RowsEvent) bool {
	if f.Databases != nil {
		inDB := false
		for _, v := range f.Databases {
			if v == e.Table.Schema {
				inDB = true
				break
			}
		}
		if !inDB {
			return true
		}
	}

	if f.Tables != nil {
		inTable := false
		for _, v := range f.Tables {
			if v == e.Table.Name {
				inTable = true
				break
			}
		}
		if !inTable {
			return true
		}
	}
	return false
}
