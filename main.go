package main

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"gopkg.in/ini.v1"
	"logagent/common"
	"logagent/etcd"
	"logagent/kafka"
	"logagent/tailfile"
)

//收集指定目录下的日志文件，发送到kafka中
//整个logagent的配置结构体
type Config struct {
	KafkaConfig   `ini:"kafka"`
	CollectConfig `ini:"collect"`
	Etcd          `ini:"etcd"`
}
type KafkaConfig struct {
	Address  string `ini:"address"`
	Topic    string `ini:"topic"`
	ChanSize int64  `ini:"chan_size"`
}
type Etcd struct {
	Address    string `ini:"address"`
	CollectKey string `ini:"collect_key"`
}
type CollectConfig struct {
	LogFilePath string `ini:"logfile_path"`
}

func run() {
	for {
		select {}
	}
}
func main() {
	// -1: 获取本机IP，为后续去etcd获取配置文件开道
	ip, err := common.GetOutboundIP()
	if err != nil {
		logrus.Errorf("get ip failed,err:%v", err)
		return
	}

	var configObj = new(Config)
	//0.读取配置文件
	//cfg,err := ini.Load("./conf/config.ini")
	//if err != nil {
	//	logrus.Error("load config failed,err:%v",err)
	//	return
	//}
	//kafkaAdd := cfg.Section("kafka").Key("address").String()
	//fmt.Println(kafkaAdd)
	err = ini.MapTo(configObj, "./conf/config.ini")
	if err != nil {
		logrus.Errorf("load config failed,err:%v", err)
		return
	}
	fmt.Printf("%#v\n", configObj)
	//1.初始化连接kafka（做好准备工作）
	err = kafka.Init([]string{configObj.KafkaConfig.Address}, configObj.KafkaConfig.ChanSize)
	if err != nil {
		logrus.Errorf("init kafka failed,err:%v", err)
		return
	}
	logrus.Info("init kafka success!")

	//初始化etcd连接
	err = etcd.Init([]string{configObj.Etcd.Address})
	if err != nil {
		logrus.Errorf("init etcd failed,err:%v", err)
		return
	}
	logrus.Info("init etcd success!")
	//从etcd中拉取要收集的日志配置项（热加载）
	collectkey := fmt.Sprintf(configObj.Etcd.CollectKey, ip)
	allConf, err := etcd.GetConf(collectkey)
	if err != nil {
		logrus.Errorf("get conf from etcd failed,err:%v", err)
		return
	}
	fmt.Println(allConf)
	//排一个小弟去监控etcd中 configObj.Etcd.CollectKey的变化
	go etcd.WatchConf(collectkey)

	//2.根据配置文件中的日志路径使用tail去收集日志
	err = tailfile.Init(allConf) //把从etcd中获取的配置项传到init中
	if err != nil {
		logrus.Errorf("init tail failed,err:%v", err)
		return
	}
	logrus.Info("init tail success!")
	//3.把日志通过sarama发往kafka
	run()
}
