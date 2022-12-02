package task

import "github.com/gridsx/datagos/meta"

/// 主要是检测， 目前哪些节点执行了哪些任务
/// 用来任务调度， 把同一个slave监听的任务分配到同一个节点上

var canalMap = make(map[string]*CanalTask, 16)

func StoreTask(t *CanalTask) {
	canalMap[t.key] = t
}

func RemoveTask(t *CanalTask) {
	canalMap[t.key] = nil
}

func GetTasks() []*meta.InstanceInfo {
	insts, err := meta.Manager.GetInstances(-1)
	if err != nil {
		return nil
	}
	for i := range insts {
		insts[i].Password = "******"
	}
	return insts
}

func GetTask(id string) *CanalTask {
	return canalMap[id]
}
