package tailfile

import (
	"github.com/sirupsen/logrus"
	"logagent/common"
)

//tailTask 的管理者

type tailTaskMgr struct {
	tailTaskMap      map[string]*tailTask       //所有的tailTask任务
	collectEntryList []common.CollectEntry      //所有的配置项
	confChan         chan []common.CollectEntry //等待新配置的通道
}

var (
	ttMgr *tailTaskMgr
)

//在main函数中调用
func Init(allConf []common.CollectEntry) (err error) {
	//allConf 里面存了若干个日志的收集项，
	//针对每一个日志收集项创建一个对应的tailObj
	ttMgr = &tailTaskMgr{
		tailTaskMap:      make(map[string]*tailTask, 20),
		collectEntryList: allConf,
		confChan:         make(chan []common.CollectEntry), //初始化新配置的管道
	}
	for _, conf := range allConf {
		tt := newTailTask(conf.Path, conf.Topic) //创建一个日志收集任务
		err = tt.Init()                          //打开日志文件准备读
		if err != nil {
			logrus.Errorf("creat tailObj for path:%s, err:%s\n", conf.Path, err)
			continue
		}
		logrus.Info("creat a tail task for path:%s success.", conf.Path)
		ttMgr.tailTaskMap[tt.path] = tt //把创建的这个tailTask任务登记在册，方便后续管理
		//开始收集日志
		go tt.run()
	}
	//初始化新配置的管道
	////阻塞的chan，没有新配置就一直等着
	//confChan = make(chan []common.CollectEntry)
	//用哨兵监听，
	//newConf := <- confChan  //取到值说明新的配置项来了
	////有新的配置项来的时候就处理一下之前启动的tailTask
	//logrus.Infof("get new conf from etcd,conf:%v",newConf)

	go ttMgr.watch() //在后台等新的配置来
	return
}

func (t *tailTaskMgr) watch() {
	for {
		//用哨兵监听，
		newConf := <-t.confChan //取到值说明新的配置项来了
		//有新的配置项来的时候就处理一下之前启动的tailTask
		logrus.Infof("get new conf from etcd,conf:%v", newConf)
		for _, conf := range newConf {
			//1、原来已经存在的任务就不管它
			if t.isExist(conf) {
				continue
			}
			//2、原来没有的新创建一个tailTask任务
			tt := newTailTask(conf.Path, conf.Topic) //创建一个日志收集任务
			err := tt.Init()                         //打开日志文件准备读
			if err != nil {
				logrus.Errorf("creat tailObj for path:%s, err:%s\n", conf.Path, err)
				continue
			}
			logrus.Info("creat a tail task for path:%s success.", conf.Path)
			ttMgr.tailTaskMap[tt.path] = tt //把创建的这个tailTask任务登记在册，方便后续管理
			//开始收集日志
			go tt.run()
		}
		//3、原来有的现在没有的要停掉这个任务
		//找出tailTaskMap中存在，但是newConf中不存在的tailTask，停掉它们
		for key, task := range t.tailTaskMap {
			var found bool
			for _, conf := range newConf {
				if key == conf.Path {
					found = true
					break
				}
			}
			if !found {
				//停掉这个tailTask
				logrus.Infof("task collect path:%v need to stop.", task.path)
				delete(t.tailTaskMap, key) //从管理类中删掉
				task.cancle()
			}
		}
	}
}

//判断tailTaskMap是否存在该收集项
func (t *tailTaskMgr) isExist(conf common.CollectEntry) bool {
	_, ok := t.tailTaskMap[conf.Path]
	return ok
}

func SendNewConf(newConf []common.CollectEntry) {
	confChan <- newConf
}
