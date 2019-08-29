package grpc

import (
	"context"
	"net"
	"time"

	pb "github.com/blastbao/goim/api/comet/grpc"
	"github.com/blastbao/goim/internal/comet"
	"github.com/blastbao/goim/internal/comet/conf"
	"github.com/blastbao/goim/internal/comet/errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// 一个 user 会位于唯一的一个 bucket 中，而一个 room 内 user 会位于多个 bucket 中，所以两个维度的消息发送方式不同:
//
// (1) 发送单用户消息，只需要根据 user 找到他归属的 bucket，进而在该 Bucket 中找出 user 对应的 Channel，然后直接塞进去即可。
// (2) 发送单房间广播，只需要遍历所有的 buckets，将消息发送到 buckets 内指定房间里的每个 user 。
// (3) 发送多房间广播，只需要遍历所有的 buckets，将消息发送到 buckets 内所有房间里的每个 user 。

// New comet grpc server.
func New(c *conf.RPCServer, s *comet.Server) *grpc.Server {
	keepParams := grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionIdle:     time.Duration(c.IdleTimeout),
		MaxConnectionAgeGrace: time.Duration(c.ForceCloseWait),
		Time:             time.Duration(c.KeepAliveInterval),
		Timeout:          time.Duration(c.KeepAliveTimeout),
		MaxConnectionAge: time.Duration(c.MaxLifeTime),
	})
	srv := grpc.NewServer(keepParams)
	pb.RegisterCometServer(srv, &server{s})
	lis, err := net.Listen(c.Network, c.Addr)
	if err != nil {
		panic(err)
	}
	go func() {
		if err := srv.Serve(lis); err != nil {
			panic(err)
		}
	}()
	return srv
}

type server struct {
	srv *comet.Server
}

var _ pb.CometServer = &server{}

// Ping Service
func (s *server) Ping(ctx context.Context, req *pb.Empty) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}

// Close Service
func (s *server) Close(ctx context.Context, req *pb.Empty) (*pb.Empty, error) {
	// TODO: some graceful close
	return &pb.Empty{}, nil
}

// PushMsg push a message to specified sub keys.
// 单人消息推送。
func (s *server) PushMsg(ctx context.Context, req *pb.PushMsgReq) (reply *pb.PushMsgReply, err error) {

	// 参数检查
	if len(req.Keys) == 0 || req.Proto == nil {
		return nil, errors.ErrPushMsgArg
	}

	// 遍历所有的消息接收者
	for _, key := range req.Keys {

		// 根据 user 找到他归属的 bucket，进而在该 Bucket 中找出 user 对应的 Channel
		if channel := s.srv.Bucket(key).Channel(key); channel != nil {

			// 检查当前 user 是否关注 req.ProtoOp 操作类型
			if !channel.NeedPush(req.ProtoOp) {
				continue
			}

			// 执行消息推送，写入 user 对应的 conn 中
			if err = channel.Push(req.Proto); err != nil {
				return
			}
		}
	}

	// 完成所有的消息推送（只写入管道，并不保证一定推送到 user），回复 Ack 消息给 Job server 。
	return &pb.PushMsgReply{}, nil
}

// Broadcast broadcast msg to all user.
func (s *server) Broadcast(ctx context.Context, req *pb.BroadcastReq) (*pb.BroadcastReply, error) {

	if req.Proto == nil {
		return nil, errors.ErrBroadCastArg
	}

	// TODO use broadcast queue
	go func() {
		// 遍历所有的 buckets，执行全局广播。
		for _, bucket := range s.srv.Buckets() {
			// 把消息 req.Proto 推送到对本 Bucket 内所有关注 req.ProtoOp 操作的 user channels 中。
			bucket.Broadcast(req.GetProto(), req.ProtoOp)
			// 广播限频参数
			if req.Speed > 0 {
				t := bucket.ChannelCount() / int(req.Speed)  // 发送总时间 = 当前 bucket 内用户总数 / 发送速率（单位时间推送的用户数）
				time.Sleep(time.Duration(t) * time.Second)   // time.sleep(发送总时间)
			}
		}
	}()
	return &pb.BroadcastReply{}, nil
}


// BroadcastRoom broadcast msg to specified room.
func (s *server) BroadcastRoom(ctx context.Context, req *pb.BroadcastRoomReq) (*pb.BroadcastRoomReply, error) {
	if req.Proto == nil || req.RoomID == "" {
		return nil, errors.ErrBroadCastRoomArg
	}

	// 因为一个 room 可能位于多个 bucket 中，所以需要遍历所有 buckets 才能将消息广播到房间内的每个 user 。
	for _, bucket := range s.srv.Buckets() {
		// 调用 bucket 内的单房间广播函数。
		bucket.BroadcastRoom(req)
	}

	return &pb.BroadcastRoomReply{}, nil
}

// Rooms gets all the room ids for the server.
func (s *server) Rooms(ctx context.Context, req *pb.RoomsReq) (*pb.RoomsReply, error) {
	var (
		roomIds = make(map[string]bool)
	)

	// 遍历所有的 bucket，收集全局的房间信息，以 map[roomid]=>true 方式返回。
	for _, bucket := range s.srv.Buckets() {
		for roomID := range bucket.Rooms() {
			roomIds[roomID] = true
		}
	}
	return &pb.RoomsReply{Rooms: roomIds}, nil
}
