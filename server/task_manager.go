package server

import (
	"errors"
	"github.com/gridsx/datagos/canal/mysql"
	"github.com/gridsx/datagos/task"
)

type manager struct {
	Id int `json:"id"` // taskId
}

func NewTask(id int) *manager {
	return &manager{Id: id}
}

// Start 异步
func (m *manager) Start() error {
	tsk, err := task.Manager.GetTask(m.Id)
	if err != nil {
		return err
	}

	switch tsk.SrcType {
	case int(task.SrcMySQL):
		canal := mysql.NewMySQLCanalTask(tsk)
		if canal == nil {
			return errors.New("error creating mysql canal")
		}
		go canal.Start()
		return nil
	case int(task.SrcMongo), int(task.SrcPostgres), int(task.SrcRedis):
		return errors.New("not supported yet")
	default:
		return errors.New("unknown src type")
	}
}

// Stop 停止
// TODO 停止后，记录position
func (m *manager) Stop() {

}

func (m *manager) Status() {

}
