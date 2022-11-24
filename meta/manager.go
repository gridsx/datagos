package meta

import (
	"encoding/json"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/gridsx/datagos/store"
	"github.com/siddontang/go-log/log"
)

var db = store.GetDb()

type metaManager struct{}

type InstanceState int
type TaskState int

const (
	InstanceCreated = 0
	InstanceRunning = 1
	InstanceStopped = 2
	InstanceDeleted = 7

	TaskInEffect = 0
	TaskDeleted  = 7
)

const (
	// 实例表操作
	getInstanceSql     = `select id, host, port, username, password, state, position,  dump_config, created, updated FROM instances WHERE state =? `
	getInstanceByIdSql = `select id, host, port, username, password, state, position,  dump_config, created, updated FROM instances WHERE id =? AND state != 7`

	updatePositionSql   = `update instances set position = ? where id = ?`
	updateInstStateSql  = `update instances set state = ? where id = ?`
	updateDumpConfigSql = `update instances set dump_config = ? WHERE  id = ? `

	// 任务表操作
	getTasks           = `select id, name,  description, instance_id, table_filter, data_filter, mapping_config, target_instance, error_continue, state, created, updated FROM mysql_tasks WHERE state = ? AND instance_id = ?`
	updateTaskStateSql = `update mysql_tasks SET state = ?  WHERE id = ?`
)

func (m *metaManager) GetInstances(state InstanceState) ([]*InstanceInfo, error) {
	rows, err := db.Query(getInstanceSql, state)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	instances := make([]*InstanceInfo, 0, 4)
	for rows.Next() {
		var inst InstanceInfo
		scanErr := rows.Scan(&inst.Id, &inst.Host, &inst.Port, &inst.Username, &inst.Password, &inst.State,
			&inst.Position, &inst.DumpConfig, &inst.Created, &inst.Updated)
		if scanErr != nil {
			log.Errorf("error scan data: %v\n", scanErr)
			continue
		}
		instances = append(instances, &inst)
	}
	return instances, nil
}

func (m *metaManager) GetInstanceById(id int) (*InstanceInfo, error) {
	row := db.QueryRow(getInstanceByIdSql, id)
	if row == nil {
		return nil, nil
	}
	var inst InstanceInfo
	scanErr := row.Scan(&inst.Id, &inst.Host, &inst.Port, &inst.Username, &inst.Password, &inst.State,
		&inst.Position, &inst.DumpConfig, &inst.Created, &inst.Updated)
	if scanErr != nil {
		return nil, nil
	}
	return &inst, nil
}

func (m *metaManager) SavePosition(instId int, position mysql.Position) error {
	// 一些情况下 pos是错的
	if position.Pos == 0 {
		return nil
	}

	d, err := json.Marshal(position)
	if err != nil {
		return err
	}
	a, err := db.Exec(updatePositionSql, string(d), instId)
	if err != nil {
		return err
	}
	r, ae := a.RowsAffected()
	if ae != nil {
		return ae
	}
	if r > 0 {
		log.Infof("metaManager.SavePosition position saved successfully: id: %d, pos: %s\n", instId, string(d))
	}
	return nil
}

func (m *metaManager) UpdateInstanceState(instId int, state int) error {
	a, err := db.Exec(updateInstStateSql, state, instId)
	if err != nil {
		return err
	}
	if r, _ := a.RowsAffected(); r > 0 {
		log.Infof("metaManager.UpdateInstanceState position saved successfully: id: %d, state: %d\n", instId, state)
	}
	return nil
}

func (m *metaManager) GetTasks(state int, instanceId int) ([]*MySQLTaskInfo, error) {
	rows, err := db.Query(getTasks, state, instanceId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	taskInfos := make([]*MySQLTaskInfo, 0, 4)
	for rows.Next() {
		var task MySQLTaskInfo
		scanErr := rows.Scan(&task.Id, &task.Name, &task.Description, &task.InstanceId, &task.TableFilter,
			&task.DataFilter, &task.MappingConfig, &task.TargetInstance, &task.ErrorContinue, &task.State, &task.Created, &task.Updated)
		if scanErr != nil {
			log.Errorf("error scan data: %v\n", scanErr)
			continue
		}
		taskInfos = append(taskInfos, &task)
	}
	return taskInfos, nil
}

func (m *metaManager) UpdateTaskState(taskId int, state int) error {
	a, err := db.Exec(updateTaskStateSql, state, taskId)
	if err != nil {
		return err
	}
	if r, _ := a.RowsAffected(); r > 0 {
		log.Infof("metaManager.UpdateTaskState state updated!: id: %d, state: %s\n", taskId, state)
	}
	return nil
}

// 当全量完成之后，此字段置空， 移除前也可以吧全量过的设置，进行一个备份
func (m *metaManager) UpdateDumpConfig(instId int, config string) error {
	a, err := db.Exec(updateDumpConfigSql, config, instId)
	if err != nil {
		return err
	}
	if r, _ := a.RowsAffected(); r > 0 {
		log.Infof("metaManager.UpdateDumpConfig config updated: id: %d, : %s\n", instId, config)
	}
	return nil
}

var Manager = &metaManager{}
