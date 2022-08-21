package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"logagent/common"
	"logagent/tailfile"
	"time"
)

var (
	client *clientv3.Client
)

func Init(address []string) (err error) {
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   address,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		// handle error!
		fmt.Printf("connect to etcd failed, err:%v\n", err)
		return
	}
	return
}

//拉取日志收集配置项的函数
func GetConf(key string) (collectEntryList []common.CollectEntry, err error) {
	ctx, cancle := context.WithTimeout(context.Background(), time.Second*2)
	defer cancle()
	resp, err := client.Get(ctx, key)
	if err != nil {
		logrus.Errorf("get conf from etcd by key:%s,err:%s\n", key, err)
		return
	}
	if len(resp.Kvs) == 0 {
		logrus.Warningf("get len:0 from etcd by key:%s\n", key)
		return
	}
	ret := resp.Kvs[0]
	//ret.Value //json格式化的字符串
	err = json.Unmarshal(ret.Value, &collectEntryList)
	if err != nil {
		logrus.Errorf("json.Unmarshal failed,err:%s\n", err)
		return
	}
	return
}

//监控etcd中日志收集项配置变化
func WatchConf(key string) {
	for {
		rch := client.Watch(context.Background(), "fantaixi") // <-chan WatchResponse
		//从通道尝试取值(监视的信息)
		for wresp := range rch {
			logrus.Info("get new conf from etcd...")
			for _, ev := range wresp.Events {
				fmt.Printf("Type: %s Key:%s Value:%s\n", ev.Type, string(ev.Kv.Key), string(ev.Kv.Value))
				var newConf []common.CollectEntry
				if ev.Type == clientv3.EventTypeDelete {
					//如果是删除
					logrus.Warning("etcd delete the key")
					tailfile.SendNewConf(newConf) //没有接受就一直是阻塞的
					continue
				}
				err := json.Unmarshal(ev.Kv.Value, &newConf)
				if err != nil {
					logrus.Errorf("Unmarshal new conf failed,err:%v\n", err)
					continue
				}
				//通知tailfile这个模块启用新配置
				tailfile.SendNewConf(newConf) //没有接受就一直是阻塞的
			}
		}
	}
}
