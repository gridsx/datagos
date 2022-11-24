package filter

type DumpFilter struct {
	Databases []string `json:"databases,omitempty"`

	// 这两个将会覆盖上面的数据库选项
	TableDB string   `json:"tableDB,omitempty"`
	Tables  []string `json:"tables,omitempty"`

	IgnoreTables []string `json:"ignoreTables,omitempty"`
	Where        string   `json:"where,omitempty"`
}
