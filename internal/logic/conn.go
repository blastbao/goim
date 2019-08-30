package logic

import (
	"context"
	"encoding/json"
	"time"

	"github.com/blastbao/goim/api/comet/grpc"
	"github.com/blastbao/goim/internal/logic/model"
	log "github.com/golang/glog"
	"github.com/google/uuid"
)

// Connect connected a conn.
// 把 user 的连接信息记录到 Redis 里。
func (l *Logic) Connect(c context.Context, server, cookie string, token []byte) (mid int64, key, roomID string, accepts []int32, hb int64, err error) {

	// 1. 把 token 解析成 client params
	var params struct {
		Mid      int64   `json:"mid"`		// client id
		Key      string  `json:"key"`		// client key
		RoomID   string  `json:"room_id"` 	// client 要进入的 room
		Platform string  `json:"platform"` 	// 设备平台
		Accepts  []int32 `json:"accepts"` 	// user 接收的 operation
	}

	if err = json.Unmarshal(token, &params); err != nil {
		log.Errorf("json.Unmarshal(%s) error(%v)", token, err)
		return
	}

	// 2. 设置返回参数: userID(mid), userKey, roomID, acceptOps, hbInterval 。
	mid = params.Mid
	roomID = params.RoomID
	accepts = params.Accepts
	hb = int64(l.c.Node.Heartbeat) * int64(l.c.Node.HeartbeatMax) 	// 告知 comet 连接多久沒心跳就直接 close
	if key = params.Key; key == "" {
		key = uuid.New().String() // 随机生成 uuid
	}

	// 3. 把 user 信息 userID(mid)，userKey，comet_server_id 保存到 redis 中。
	if err = l.dao.AddMapping(c, mid, key, server); err != nil {
		log.Errorf("l.dao.AddMapping(%d,%s,%s) error(%v)", mid, key, server, err)
	}

	log.Infof("conn connected key:%s server:%s mid:%d token:%s", key, server, mid, token)
	return
}


// Disconnect disconnect a conn.
// 从 Redis 中删除 user 的连接信息。
func (l *Logic) Disconnect(c context.Context, mid int64, key, server string) (has bool, err error) {
	if has, err = l.dao.DelMapping(c, mid, key, server); err != nil {
		log.Errorf("l.dao.DelMapping(%d,%s) error(%v)", mid, key, server)
		return
	}
	log.Infof("conn disconnected key:%s server:%s mid:%d", key, server, mid)
	return
}

// Heartbeat heartbeat a conn.
// 更新某个 user 的 redis 过期时间。
func (l *Logic) Heartbeat(c context.Context, mid int64, key, server string) (err error) {
	has, err := l.dao.ExpireMapping(c, mid, key)
	if err != nil {
		log.Errorf("l.dao.ExpireMapping(%d,%s,%s) error(%v)", mid, key, server, err)
		return
	}

	// 不存在 or 没更新成功 就直接覆盖。
	if !has {
		if err = l.dao.AddMapping(c, mid, key, server); err != nil {
			log.Errorf("l.dao.AddMapping(%d,%s,%s) error(%v)", mid, key, server, err)
			return
		}
	}
	log.Infof("conn heartbeat key:%s server:%s mid:%d", key, server, mid)
	return
}

// RenewOnline renew a server online.
//
//
func (l *Logic) RenewOnline(c context.Context, server string, roomCount map[string]int32) (map[string]int32, error) {

	online := &model.Online{
		Server:    server, 				// comet 服务名
		RoomCount: roomCount,			// 房间 room id  => online user count
		Updated:   time.Now().Unix(), 	// timestamp
	}

	//
	if err := l.dao.AddServerOnline(context.Background(), server, online); err != nil {
		return nil, err
	}

	return l.roomCount, nil
}

// Receive receive a message.
func (l *Logic) Receive(c context.Context, mid int64, proto *grpc.Proto) (err error) {
	log.Infof("receive mid:%d message:%+v", mid, proto)
	return
}
