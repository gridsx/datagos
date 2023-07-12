package mapper

type TableMapping struct {
	Database    string       `json:"database,omitempty"`
	SrcTable    string       `json:"srcTable,omitempty"`
	DstTable    string       `json:"dstTable,omitempty"`
	ColMappings []ColMapping `json:"colMappings,omitempty"`
}

// ColMapping 列Mapping， 映射转换列用
type ColMapping struct {
	Src string `json:"src,omitempty"`
	Dst string `json:"dst,omitempty"`
	// 转换所使用的表达式， 没有表达式，则默认一对一转换
	Expr string `json:"expr,omitempty"`
}
