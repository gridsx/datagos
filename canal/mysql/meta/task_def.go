package meta

import (
	"encoding/json"
	"time"

	"github.com/gridsx/datagos/canal/mysql/filter"
	"github.com/gridsx/datagos/canal/mysql/mapper"
	"github.com/siddontang/go-log/log"
)

type MySQLTaskInfo struct {
	Id             int    `json:"id"`
	Name           string `json:"name"`
	Description    string `json:"description"`
	InstanceId     int    `json:"instanceId"`
	TableFilter    string `json:"tableFilter"`
	DataFilter     string `json:"dataFilter"`
	MappingConfig  string `json:"mappingConfig"`
	TargetInstance string `json:"targetInstance"`
	ErrorContinue  int
	State          int       `json:"state"`
	Created        time.Time `json:"created"`
	Updated        time.Time `json:"updated"`
}

func (t *MySQLTaskInfo) Filters() []filter.MySQLFilter {
	dataFilters := make([]*filter.EventDataFilter, 0, 4)
	if len(t.DataFilter) > 0 {
		err := json.Unmarshal([]byte(t.DataFilter), &dataFilters)
		if err != nil {
			log.Warnf("Filters convert data filter failed: %s\n", t.DataFilter)
		}
	}
	tableFilters := make([]*filter.TableFilter, 0, 4)
	if len(t.TableFilter) > 0 {
		err := json.Unmarshal([]byte(t.TableFilter), &tableFilters)
		if err != nil {
			log.Warnf("Filters convert table filter failed: %s\n", t.TableFilter)
		}
	}
	filters := make([]filter.MySQLFilter, 0, 4)

	if len(dataFilters) > 0 {
		for _, v := range dataFilters {
			filters = append(filters, v)
		}
	}
	if len(tableFilters) > 0 {
		for _, v := range tableFilters {
			filters = append(filters, v)
		}
	}
	return filters
}

func (t *MySQLTaskInfo) Mappings() []mapper.TableMapping {
	if len(t.MappingConfig) == 0 {
		return nil
	}
	mappings := make([]mapper.TableMapping, 0, 4)
	err := json.Unmarshal([]byte(t.MappingConfig), &mappings)
	if err != nil {
		log.Errorf("convert mappings error, config: %s\n", t.MappingConfig)
		return nil
	}
	return mappings
}

func (t *MySQLTaskInfo) TargetDB() *MySQLInstance {
	if len(t.TargetInstance) > 0 {
		var instance MySQLInstance
		err := json.Unmarshal([]byte(t.TargetInstance), &instance)
		if err != nil {
			log.Errorf("convert target instance error: %s\n", t.TargetInstance)
			return nil
		}
		return &instance
	}
	return nil
}

func (t *MySQLTaskInfo) ToMySQLSinkerConfig() *MySQLSinkerConfig {
	return &MySQLSinkerConfig{
		DestDatasource: *t.TargetDB(),
		Filters:        t.Filters(),
		Mappings:       t.Mappings(),
		ErrorContinue:  true,
	}
}
