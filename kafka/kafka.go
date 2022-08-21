package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)
var(
	client sarama.SyncProducer
	msgChan chan *sarama.ProducerMessage
)
//初始化全局的 kafka client
func Init(address []string,chanSiez int64) (err error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          // 发送完数据需要leader和follow都确认
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 新选出一个partition
	config.Producer.Return.Successes = true                   // 成功交付的消息将在success channel返回

	// 连接kafka
	client, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		logrus.Error("producer closed, err:", err)
		return
	}
	//初始化MsgChan
	msgChan = make(chan *sarama.ProducerMessage,chanSiez)
	//起一个后台的goroutine从msgchan中读数据
	go SendMsg()
	return
}

//从MsgChan中读取msg，发送给kafka
func SendMsg() {
	for{
		select {
		case msg := <-msgChan:
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				logrus.Warning("send msg failed, err:", err)
				return
			}
			logrus.Info("send msg success,pid:%v offset:%v\n", pid, offset)
		}
	}
}

//定义一个函数向外暴露msgChan
func ToMsgChan(msg *sarama.ProducerMessage)   {
	msgChan <- msg
}