package filter

import (
	"strings"

	"github.com/go-mysql-org/go-mysql/canal"
)

type TableFilter struct {
	IgnoreTables    []string `json:"ignoreTables,omitempty"`
	IgnoreActions   []string `json:"ignoreActions,omitempty"`
	IgnoreDatabases []string `json:"ignoreDatabases,omitempty"`

	// 同步哪些表， 配置此选项，则上述所有选项将失效， 只用这一选项
	IncludeTables []string `json:"includeTables,omitempty"`
}

func (f *TableFilter) tableIgnored(name string) bool {
	return inList(name, f.IgnoreTables)
}

func (f *TableFilter) schemaIgnored(name string) bool {
	return inList(name, f.IgnoreDatabases)
}

func (f *TableFilter) actionIgnored(name string) bool {
	return inList(name, f.IgnoreActions)
}

func (f *TableFilter) Match(e *canal.RowsEvent) bool {
	if len(f.IncludeTables) > 0 {
		return !inList(e.Table.Name, f.IncludeTables)
	}
	return f.tableIgnored(e.Table.Name) || f.schemaIgnored(e.Table.Schema) || f.actionIgnored(e.Action)
}

func inList(name string, list []string) bool {
	if len(list) == 0 {
		return false
	}
	for _, v := range list {
		if strings.EqualFold(v, name) {
			return true
		}
	}
	return false
}
