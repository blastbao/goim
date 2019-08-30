package logic

import (
	"context"

	"github.com/blastbao/goim/internal/logic/model"

	log "github.com/golang/glog"
)





// PushKeys push a message by keys.

// 根据 user key 推送消息
func (l *Logic) PushKeys(c context.Context, op int32, keys []string, msg []byte) (err error) {


	// 取出 user keys 所在的 servers
	servers, err := l.dao.ServersByKeys(c, keys)
	if err != nil {
		return
	}

	// pushKeys 用于保存 map[ server => user keys ] 的映射关系。
	pushKeys := make(map[string][]string)

	// 根据 keys 和 servers 来填充 pushKeys 。
	for i, key := range keys {
		server := servers[i]
		if server != "" && key != "" {
			pushKeys[server] = append(pushKeys[server], key)
		}
	}

	// 根据 server 和 user keys 来推送消息，把消息发送到 kafka。
	for server := range pushKeys {
		if err = l.dao.PushMsg(c, op, server, pushKeys[server], msg); err != nil {
			return
		}
	}

	return
}





// PushMids push a message by mid.
// 根据 user id 推送消息
func (l *Logic) PushMids(c context.Context, op int32, mids []int64, msg []byte) (err error) {


	// 根据 user ids 获取到 map[ user key => server ]
	keyServers, _, err := l.dao.KeysByMids(c, mids)
	if err != nil {
		return
	}

	// 把 map[ user key => server ] 转换为 map[ server => user keys ]
	keys := make(map[string][]string)
	for key, server := range keyServers {
		if key == "" || server == "" {
			log.Warningf("push key:%s server:%s is empty", key, server)
			continue
		}
		keys[server] = append(keys[server], key)
	}

	// 根据 server 和 user key 来推送消息，把消息发送到 kafka。
	for server, keys := range keys {
		// 发送消息到 kafka。
		if err = l.dao.PushMsg(c, op, server, keys, msg); err != nil {
			return
		}
	}
	return
}

// PushRoom push a message by room.
// 单房间广播推送。
func (l *Logic) PushRoom(c context.Context, op int32, typ, room string, msg []byte) (err error) {
	// 发送消息到 kafka。
	return l.dao.BroadcastRoomMsg(c, op, model.EncodeRoomKey(typ, room), msg)
}

// PushAll push a message to all.
// 全局房间广播推送，但有限制 operation 。
func (l *Logic) PushAll(c context.Context, op, speed int32, msg []byte) (err error) {
	// 发送消息到 kafka。
	return l.dao.BroadcastMsg(c, op, speed, msg)
}
