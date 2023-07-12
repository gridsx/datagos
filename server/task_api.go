package server

import (
	"github.com/kataras/iris/v12"
	"github.com/winjeg/irisword/ret"
)

func startTask(ctx iris.Context) {
	taskId, _ := ctx.URLParamInt("id")
	t := NewTask(taskId)
	if err := t.Start(); err != nil {
		ret.ServerError(ctx, err.Error())
		return
	}
	ret.Ok(ctx)
}
