package server

import (
	"fmt"

	"github.com/gridsx/datagos/config"
	"github.com/kataras/iris/v12"
	"github.com/kataras/iris/v12/middleware/recover"
	"github.com/siddontang/go-log/log"
)

var conf = config.GetConf()

func Serve() {
	app := iris.New()
	app.Use(recover.New())

	api := app.Party("/api")
	{
		api.Get("/task/start", startTask)
	}

	err := app.Listen(fmt.Sprintf(":%d", conf.Server.Port))
	if err != nil {
		log.Errorf("Serve error :%v\n", err)
	}
}
