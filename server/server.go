package server

import (
	"fmt"
	"github.com/gridsx/datagos/config"
	"github.com/kataras/iris/v12"
	"github.com/siddontang/go-log/log"
)

var conf = config.GetConf()

func Serve() {
	app := iris.New()

	api := app.Party("/api")
	{
		api.Get("/tasks", taskList)
		api.Get("/task", taskDetail)
		api.Get("/task/stop", stopTask)
		api.Get("/task/start", startTask)
		api.Post("/task", addTask)
	}

	err := app.Listen(fmt.Sprintf(":%d", conf.Server.Port))
	if err != nil {
		log.Errorf("Serve error :%v\n", err)
	}
}
