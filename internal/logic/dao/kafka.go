package dao

import (
	"context"
	"strconv"

	pb "github.com/blastbao/goim/api/logic/grpc"
	"github.com/gogo/protobuf/proto"
	log "github.com/golang/glog"
	sarama "gopkg.in/Shopify/sarama.v1"
)

// 	PB 定义:
//
// 	message PushMsg {
//
//		// 消息类型
//		enum Type {
//			PUSH = 0;       // 单用户
//			ROOM = 1;       // 单房间
//			BROADCAST = 2;  // 全局广播
//		}
//
//		Type type = 1;              // 消息类型
//		int32 operation = 2;        // 操作类型
//		int32 speed = 3;            // 限频参数
//		string server = 4;          // 服务器 IP
//		string room = 5;            // 房间 ID
//		repeated string keys = 6;   // 用户 key 列表
//		bytes msg = 7;              // 消息体
// 	}

// PushMsg push a message to databus.
func (d *Dao) PushMsg(c context.Context, op int32, server string, keys []string, msg []byte) (err error) {
	pushMsg := &pb.PushMsg{
		Type:      pb.PushMsg_PUSH,
		Operation: op,
		Server:    server,
		Keys:      keys,
		Msg:       msg,
	}
	b, err := proto.Marshal(pushMsg)
	if err != nil {
		return
	}
	m := &sarama.ProducerMessage{
		Key:   sarama.StringEncoder(keys[0]),
		Topic: d.c.Kafka.Topic,
		Value: sarama.ByteEncoder(b),
	}
	if _, _, err = d.kafkaPub.SendMessage(m); err != nil {
		log.Errorf("PushMsg.send(push pushMsg:%v) error(%v)", pushMsg, err)
	}
	return
}

// BroadcastRoomMsg push a message to databus.
func (d *Dao) BroadcastRoomMsg(c context.Context, op int32, room string, msg []byte) (err error) {


	pushMsg := &pb.PushMsg{
		Type:      pb.PushMsg_ROOM,
		Operation: op,
		Room:      room,
		Msg:       msg,
	}

	b, err := proto.Marshal(pushMsg)
	if err != nil {
		return
	}

	m := &sarama.ProducerMessage{
		Key:   sarama.StringEncoder(room),
		Topic: d.c.Kafka.Topic,
		Value: sarama.ByteEncoder(b),
	}
	if _, _, err = d.kafkaPub.SendMessage(m); err != nil {
		log.Errorf("PushMsg.send(broadcast_room pushMsg:%v) error(%v)", pushMsg, err)
	}
	return
}

// BroadcastMsg push a message to databus.
func (d *Dao) BroadcastMsg(c context.Context, op, speed int32, msg []byte) (err error) {
	pushMsg := &pb.PushMsg{
		Type:      pb.PushMsg_BROADCAST,
		Operation: op,
		Speed:     speed,
		Msg:       msg,
	}
	b, err := proto.Marshal(pushMsg)
	if err != nil {
		return
	}
	m := &sarama.ProducerMessage{
		Key:   sarama.StringEncoder(strconv.FormatInt(int64(op), 10)),
		Topic: d.c.Kafka.Topic,
		Value: sarama.ByteEncoder(b),
	}
	if _, _, err = d.kafkaPub.SendMessage(m); err != nil {
		log.Errorf("PushMsg.send(broadcast pushMsg:%v) error(%v)", pushMsg, err)
	}
	return
}
