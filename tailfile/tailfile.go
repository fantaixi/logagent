package tailfile

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
	"logagent/common"
	"logagent/kafka"
	"strings"
	"time"
)

type tailTask struct {
	path   string
	topic  string
	tObj   *tail.Tail
	ctx    context.Context
	cancle context.CancelFunc
}

var (
	confChan chan []common.CollectEntry
)

func newTailTask(path, topic string) *tailTask {
	ctx, cancle := context.WithCancel(context.Background())
	tt := &tailTask{
		path:   path,
		topic:  topic,
		ctx:    ctx,
		cancle: cancle,
	}
	return tt
}

func (t *tailTask) Init() (err error) {
	cfg := tail.Config{
		ReOpen:    true,                                 // 重新打开
		Follow:    true,                                 // 是否跟随
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2}, // 从文件的哪个地方开始读
		MustExist: false,                                // 文件不存在不报错
		Poll:      true,
	}
	t.tObj, err = tail.TailFile(t.path, cfg)
	return
}

func (t *tailTask) run() {
	logrus.Infof("collect for path:%s is runing...", t.path)
	// logfile --> tailObj --> log --> Cilent -->kafka
	for {
		select {
		case <-t.ctx.Done(): //只要调用t.cancle() 就会收到信号
			logrus.Infof("path:%s is stopping...", t.path)
			return
		case line, ok := <-t.tObj.Lines: //遍历chan，读取日志内容
			if !ok {
				logrus.Warn("tail file close reopen, path:%s\n", t.path)
				time.Sleep(time.Second)
				continue
			}
			//如果是空行就略过
			if len(strings.Trim(line.Text, "\r")) == 0 {
				logrus.Info("出现空行，直接跳过。。。")
				continue
			}
			//利用通道将同步的代码改为异步的
			//把读出来的一行日志包装到kafka里面的msg类型
			// 构造一个消息
			msg := &sarama.ProducerMessage{}
			msg.Topic = t.topic //每个tailObj自己的topic
			msg.Value = sarama.StringEncoder(line.Text)
			//丢到通道中
			kafka.ToMsgChan(msg)
		}
	}
}
