package job

import (
	"context"
	"fmt"

	comet "github.com/blastbao/goim/api/comet/grpc"
	pb "github.com/blastbao/goim/api/logic/grpc"
	"github.com/blastbao/goim/pkg/bytes"
	log "github.com/golang/glog"
)



// 推送消息到 comet server
func (j *Job) push(ctx context.Context, pushMsg *pb.PushMsg) (err error) {
	switch pushMsg.Type {
	case pb.PushMsg_PUSH: 		// 单人消息，需要指定 comet_svr_id，然后发给该 comet.pushChan[i] 中。
		err = j.pushKeys(pushMsg.Operation, pushMsg.Server, pushMsg.Keys, pushMsg.Msg)
	case pb.PushMsg_ROOM: 		// 单房间广播，需要通过 Room 对象来进行消息的缓存、聚合、发送。
		err = j.getRoom(pushMsg.Room).Push(pushMsg.Operation, pushMsg.Msg)
	case pb.PushMsg_BROADCAST: 	// 全局广播，直接消息塞到 comet.broadcastChan 管道中。
		err = j.broadcast(pushMsg.Operation, pushMsg.Msg, pushMsg.Speed)
	default:
		err = fmt.Errorf("no match push type: %s", pushMsg.Type)
	}
	return
}

// pushKeys push a message to a batch of subkeys.
// 发送单用户消息。
func (j *Job) pushKeys(operation int32, serverID string, subKeys []string, body []byte) (err error) {

	buf := bytes.NewWriterSize(len(body) + 64)

	p := &comet.Proto{
		Ver:  1, 			// 版本号
		Op:   operation, 	// 房间号
		Body: body, 		// 消息体
	}

	p.WriteTo(buf)
	p.Body = buf.Buffer()
	p.Op = comet.OpRaw

	var args = comet.PushMsgReq{
		Keys:    subKeys,
		ProtoOp: operation,
		Proto:   p,
	}

	// 根据 serverID 确定 commet svr，然后把消息塞给他。
	if c, ok := j.cometServers[serverID]; ok {
		if err = c.Push(&args); err != nil {
			log.Errorf("c.Push(%v) serverID:%s error(%v)", args, serverID, err)
		}
		log.Infof("pushKey:%s comets:%d", serverID, len(j.cometServers))
	}

	return
}

// broadcast broadcast a message to all.
// 发送全局广播消息。
//
// 具体的发送逻辑，是把消息塞到 comet.broadcastChan 管道中，comet 有协程负责真正的发送。
func (j *Job) broadcast(operation int32, body []byte, speed int32) (err error) {

	buf := bytes.NewWriterSize(len(body) + 64)
	p := &comet.Proto{
		Ver:  1,			// uint32
		Op:   operation,	// uint32
		Body: body,			// len(body)
	}
	p.WriteTo(buf)
	p.Body = buf.Buffer() 	// ？
	p.Op = comet.OpRaw		// ？

	// 取出当前活跃 cometSvrs 列表，计算发送速率
	comets := j.cometServers
	speed /= int32(len(comets))
	// 构造广播消息请求
	var args = comet.BroadcastReq{
		ProtoOp: operation,	// 操作类型
		Proto:   p,			// 消息
		Speed:   speed,		// 发送速率
	}
	// 对每个 commetSvr 进行广播消息的推送。
	for serverID, c := range comets {
		if err = c.Broadcast(&args); err != nil {
			log.Errorf("c.Broadcast(%v) serverID:%s error(%v)", args, serverID, err)
		}
	}
	log.Infof("broadcast comets:%d", len(comets))
	return
}

// broadcastRoomRawBytes broadcast aggregation messages to room.
// 负责 `单房间广播消息` 的发送。
//
// 备注，根据主逻辑 push() 函数能够看到，对于 `单房间广播消息` 的发送，是通过 Room 对象间接调用的。
// 具体的发送逻辑，是把消息塞到 comet.roomChan[idx] 管道中，comet 有协程负责真正的发送。
func (j *Job) broadcastRoomRawBytes(roomID string, body []byte) (err error) {

	// 构造聚合广播消息请求
	args := comet.BroadcastRoomReq{
		RoomID: roomID, 		// 房间 ID
		Proto: &comet.Proto{ 	// 消息
			Ver:  1,			// 版本号
			Op:   comet.OpRaw,	// 消息类型
			Body: body,			// 消息体
		},
	}

	// 取出当前活跃 cometSvrs 列表，对每个 cometSvr 进行消息推送。
	comets := j.cometServers
	for serverID, c := range comets {
		if err = c.BroadcastRoom(&args); err != nil {
			log.Errorf("c.BroadcastRoom(%v) roomID:%s serverID:%s error(%v)", args, roomID, serverID, err)
		}
	}
	log.Infof("broadcastRoom comets:%d", len(comets))
	return
}
