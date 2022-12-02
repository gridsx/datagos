package server

import (
	"fmt"
	"github.com/gridsx/datagos/meta"
	"github.com/gridsx/datagos/task"
	"github.com/kataras/iris/v12"
	"github.com/siddontang/go-log/log"
	"github.com/winjeg/irisword/ret"
)

func taskList(ctx iris.Context) {
	ret.Ok(ctx, task.GetTasks())
}

func taskDetail(ctx iris.Context) {
	taskId := ctx.URLParam("id")
	t := task.GetTask(taskId)
	if t == nil {
		ret.BadRequest(ctx, "task doesn't exist")
		return
	}
	result := make(map[string]interface{}, 4)
	sinkCfg, _ := meta.Manager.GetTasks(meta.TaskInEffect, t.Inst.Id)
	result["instance"] = t.Inst
	result["delay"] = t.GetDelay()
	result["running"] = t.Running()
	result["sinkers"] = sinkCfg
	ret.Ok(ctx, result)
}

// 对于新增任务
// 1. 指定binlog 位点，为了避免影响其他，当追平的时候， 找到同一个数据库实例对象进行merge, 从 position 较小的开始消费即可
// 2. 从最新binlog位点开始消费， stop/update/start
// 3. 存在全量的，为了避免影响其他， 单开slave进行同步， 当追平的时候， 找到同样的对象进行merge， 从binlog position 较小的开始消费
func addTask(ctx iris.Context) {
	ret.Ok(ctx, "not implemented")
}

func startTask(ctx iris.Context) {
	instId, _ := ctx.URLParamInt("id")

	if task.GetTask(fmt.Sprintf("%d", instId)) != nil {
		ret.Ok(ctx, "task already started")
		return
	}

	server, err := meta.Manager.GetInstanceById(instId)
	if server == nil {
		ret.Ok(ctx, "task not found.")
		return
	}
	if err != nil {
		ret.ServerError(ctx, err.Error())
		return
	}
	mysqlTasks, tErr := meta.Manager.GetTasks(meta.TaskInEffect, instId)
	if tErr != nil {
		ret.ServerError(ctx, err.Error())
		return
	}
	sinkers := make([]meta.MySQLSinkerConfig, 0, 4)
	for _, t := range mysqlTasks {
		sinkers = append(sinkers, *t.ToMySQLSinkerConfig())
	}
	canalTask := task.NewMySQLCanalTask(server, sinkers)
	go func(t *task.CanalTask) {
		defer func() {
			if err := recover(); err != nil {
				log.Errorf(fmt.Sprintf("ERROR =============================== sync stop panic %v==========================================\n", err))
			}
		}()
		stErr := t.Start()
		if stErr != nil {
			log.Errorf("error starting task， error,", stErr)
		}
		defer t.Stop()

	}(canalTask)
	ret.Ok(ctx)
}

func stopTask(ctx iris.Context) {
	taskId := ctx.URLParam("id")
	t := task.GetTask(taskId)
	if t == nil {
		ret.NotFound(ctx)
		return
	}
	t.Stop()
	ret.Ok(ctx)
}
