package task

import (
	"github.com/gridsx/datagos/store"
	"github.com/winjeg/go-commons/log"
)

var (
	db     = store.GetDb()
	logger = log.GetLogger(nil)
)

type metaManager struct{}

const (
	updatePositionSql  = `update tasks set info = ? where id = ?`
	updateInstStateSql = `update tasks set state = ? where id = ?`
	taskDetailSql      = `select id, title, src_type, src, dest, state, info, created, updated from tasks where id = ?`
	taskListSql        = `select id, title, src_type, src, dest, state, info, created, updated from tasks LIMIT ?, ?`
)

var Manager = &metaManager{}

func (tm *metaManager) GetTasks(size int, page int) ([]*Task, error) {
	if size > 50 || size < 0 {
		size = 10
	}
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * size
	rows, err := db.Query(taskListSql, offset, size)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tasks := make([]*Task, 0, size)
	for rows.Next() {
		task := new(Task)
		if err := rows.Scan(&task.Id, &task.Title, &task.SrcType, &task.Src, &task.Dest,
			&task.State, &task.Info, &task.Created, &task.Updated); err != nil {
			continue
		}
		tasks = append(tasks, task)
	}
	return tasks, err
}

func (tm *metaManager) GetTask(id int) (*Task, error) {
	row := db.QueryRow(taskDetailSql, id)
	task := new(Task)
	if err := row.Scan(&task.Id, &task.Title, &task.SrcType, &task.Src, &task.Dest,
		&task.State, &task.Info, &task.Created, &task.Updated); err != nil {
		return nil, err
	}
	return task, nil
}
