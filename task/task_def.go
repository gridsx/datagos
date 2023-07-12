package task

import (
	"fmt"
	"time"
)

type srcType int

const (
	Running = 1
	Stopped = 2
)

const (
	_ srcType = iota
	SrcMySQL
	SrcPostgres
	SrcMongo
	SrcRedis
)

const getDestSQL = `select id, type, name, config, created, updated from task_dests where id IN (%s)`

// Task 对应数据库表 tasks
type Task struct {
	Id      int       `json:"id" gorm:"id"`
	Title   string    `json:"title" gorm:"title"`
	SrcType int       `json:"srcType" gorm:"src_type"`
	Src     string    `json:"src" gorm:"src"`
	Dest    string    `json:"dest" gorm:"dest"`
	State   int       `json:"state" gorm:"state"`
	Info    *string   `json:"info" gorm:"info"`
	Created time.Time `json:"created" gorm:"created"`
	Updated time.Time `json:"updated" gorm:"updated"`
}

func (t *Task) TableName() string {
	return "tasks"
}

func (t *Task) UpdateTaskInfo(info string) error {
	_, err := db.Exec(updatePositionSql, info, t.Id)
	return err
}

func (t *Task) UpdateTaskState(state int) error {
	a, err := db.Exec(updateInstStateSql, state, t.Id)
	if err != nil {
		return err
	}
	if r, _ := a.RowsAffected(); r > 0 {
		logger.Infof("metaManager.UpdateTaskState position saved successfully: id: %d, state: %d\n", t.Id, state)
	}
	return nil
}

func (t *Task) GetDest() ([]*Dest, error) {
	if len(t.Dest) == 0 {
		return nil, nil
	}
	rows, err := db.Query(fmt.Sprintf(getDestSQL, t.Dest))
	if err != nil {
		return nil, err
	}
	destinations := make([]*Dest, 0, 4)
	for rows.Next() {
		dest := new(Dest)
		err := rows.Scan(&dest.Id, &dest.Type, &dest.Name, &dest.Config, &dest.Created, &dest.Updated)
		if err != nil {
			continue
		}
		destinations = append(destinations, dest)
	}
	return destinations, nil
}

type destType int

const (
	_ destType = iota
	DestMySQL
	DestPostgres
	DestRedis
	DestEs
	DestMongo
	DestRocketMQ
)

type Dest struct {
	Id      int       `json:"id,omitempty" gorm:"id"`
	Type    int       `json:"type,omitempty" gorm:"type"`
	Name    string    `json:"name,omitempty" gorm:"name"`
	Config  string    `json:"config,omitempty" gorm:"config"`
	Created time.Time `json:"created" gorm:"created"`
	Updated time.Time `json:"updated" gorm:"updated"`
}

func (d *Dest) TableName() string {
	return "task_dests"
}
