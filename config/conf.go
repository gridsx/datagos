package config

import (
	config "github.com/winjeg/go-commons/conf"
	"sync"
)

var (
	once sync.Once
	conf *Config
)

func GetConf() *Config {
	if conf != nil {
		return conf
	} else {
		once.Do(getConf)
	}
	return conf
}

type MysqlConfig struct {
	Host     string `json:"host" yaml:"host"`
	Port     int    `json:"port" yaml:"port"`
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
	Database string `json:"database,omitempty" yaml:"database"`
}

type ServerConfig struct {
	Port int `json:"port" yaml:"port"`
}

type Config struct {
	Mysql  MysqlConfig  `json:"mysql", yaml:"mysql"`
	Server ServerConfig `json:"server" yaml:"server"`
}

const configFile = "conf.yaml"

func getConf() {
	conf = new(Config)
	err := config.Yaml2Object(configFile, &conf)
	if err != nil {
		panic(err)
	}
}
