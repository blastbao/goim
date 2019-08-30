package job

import (
	"context"
	"fmt"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/bilibili/discovery/naming"
	comet "github.com/blastbao/goim/api/comet/grpc"
	"github.com/blastbao/goim/internal/job/conf"

	log "github.com/golang/glog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	// grpc options
	grpcKeepAliveTime    = time.Duration(10) * time.Second
	grpcKeepAliveTimeout = time.Duration(3)  * time.Second
	grpcBackoffMaxDelay  = time.Duration(3)  * time.Second
	grpcMaxSendMsgSize   = 1 << 24
	grpcMaxCallMsgSize   = 1 << 24
)

const (
	// grpc options
	grpcInitialWindowSize     = 1 << 24
	grpcInitialConnWindowSize = 1 << 24
)

func newCometClient(addr string) (comet.CometClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second))
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr,
		[]grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithInitialWindowSize(grpcInitialWindowSize),
			grpc.WithInitialConnWindowSize(grpcInitialConnWindowSize),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(grpcMaxCallMsgSize)),
			grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(grpcMaxSendMsgSize)),
			grpc.WithBackoffMaxDelay(grpcBackoffMaxDelay),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                grpcKeepAliveTime,
				Timeout:             grpcKeepAliveTimeout,
				PermitWithoutStream: true,
			}),
		}...,
	)
	if err != nil {
		return nil, err
	}
	return comet.NewCometClient(conn), err
}

// Comet is a comet.
type Comet struct {

	// 某 Comet server 的 ip or name
	serverID      string

	// Comet grpc client
	client        comet.CometClient

	// 推送单人消息給 Comet 的 chan，每个 process 协程负责一个 chan，发送时可以选择任意一个 chan，默认是轮训选择。
	pushChan      []chan *comet.PushMsgReq
	// 推送单房间广播消息给 Comet 的 chan，每个 process 协程负责一个 chan，发送时可以选择任意一个 chan，默认是轮训选择。
	roomChan      []chan *comet.BroadcastRoomReq
	// 推送多房间广播消息给 Comet 的 chan，全局只有一个，多个 process 协程争抢消费，抢到则负责发送。
	broadcastChan chan *comet.BroadcastReq

	// 负责将单人消息推送至 Comet 的 goroutine 数目
	pushChanNum   uint64
	// 负责将单房间消息推送至 Comet 的 goroutine 数目
	roomChanNum   uint64

	// 开多少 goroutine 来并发推送消息给 Comet
	routineSize   uint64

	ctx    context.Context
	cancel context.CancelFunc
}

// NewComet new a comet.
func NewComet(in *naming.Instance, c *conf.Comet) (*Comet, error) {
	cmt := &Comet{
		serverID:      in.Hostname,
		pushChan:      make([]chan *comet.PushMsgReq, c.RoutineSize),		// 创建数组
		roomChan:      make([]chan *comet.BroadcastRoomReq, c.RoutineSize),	// 创建数组
		broadcastChan: make(chan *comet.BroadcastReq, c.RoutineSize),		// 创建广播管道
		routineSize:   uint64(c.RoutineSize),
	}

	// 找出 Comet server 的 addr
	var grpcAddr string
	for _, addrs := range in.Addrs {
		u, err := url.Parse(addrs)
		if err == nil && u.Scheme == "grpc" {
			grpcAddr = u.Host
		}
	}
	if grpcAddr == "" {
		return nil, fmt.Errorf("invalid grpc address:%v", in.Addrs)
	}

	// 跟 Comet servers 建立 grpc 连接，得到 grpc client
	var err error
	if cmt.client, err = newCometClient(grpcAddr); err != nil {
		return nil, err
	}
	cmt.ctx, cmt.cancel = context.WithCancel(context.Background())

	// 创建 c.RoutineSize 个 goroutine 执行，并行的发送消息给 Comet 。
	for i := 0; i < c.RoutineSize; i++ {

		cmt.pushChan[i] = make(chan *comet.PushMsgReq, c.RoutineChan)			// 创建单人消息推送管道
		cmt.roomChan[i] = make(chan *comet.BroadcastRoomReq, c.RoutineChan)		// 创建单房间消息推送管道

		go cmt.process(cmt.pushChan[i], cmt.roomChan[i], cmt.broadcastChan) 	// 创建 goroutine 负责消息推送
	}
	return cmt, nil
}

// Push push a user message.
//
// 单人消息推送需要交由某个负责单人消息推送的 goroutine 来处理， Comet 内会预先开启多个 goroutine ，
// 每个 goroutine 内都有一个用于单人消息推送的 chan ，这里用 原子锁 + 序号自增（轮循）的方式选择一个 goroutine，
// 让其负责消息发送。
func (c *Comet) Push(arg *comet.PushMsgReq) (err error) {
	idx := atomic.AddUint64(&c.pushChanNum, 1) % c.routineSize
	c.pushChan[idx] <- arg
	return
}

// BroadcastRoom broadcast a room message.
//
// 单房间消息推送，逻辑同上。
func (c *Comet) BroadcastRoom(arg *comet.BroadcastRoomReq) (err error) {
	idx := atomic.AddUint64(&c.roomChanNum, 1) % c.routineSize
	c.roomChan[idx] <- arg
	return
}

// Broadcast broadcast a message.
//
// 全局广播消息推送。
func (c *Comet) Broadcast(arg *comet.BroadcastReq) (err error) {
	c.broadcastChan <- arg
	return
}


// 负责将消息推送给 comet server 的协程函数
func (c *Comet) process(pushChan chan *comet.PushMsgReq, roomChan chan *comet.BroadcastRoomReq, broadcastChan chan *comet.BroadcastReq) {
	for {

		// 每个单独的 process 协程都要监听 pushChan、roomChan、broadcastChan 三个消息通道，其中 broadcastChan 是所有协程共享的。
		select {

		// 1. 多房间广播
		case broadcastArg := <-broadcastChan:
			// 发送广播消息给 comet svr，comet svr 内部会遍历所有的 buckets，执行全局广播。
			_, err := c.client.Broadcast(context.Background(), &comet.BroadcastReq{
				Proto:   broadcastArg.Proto,   // 消息体
				ProtoOp: broadcastArg.ProtoOp, // 房间号
				Speed:   broadcastArg.Speed,   // 广播限频参数
			})
			if err != nil {
				log.Errorf("c.client.Broadcast(%s, reply) serverId:%s error(%v)", broadcastArg, c.serverID, err)
			}

		// 2. 单房间推送
		case roomArg := <-roomChan:

			// 发送单房间广播消息给 comet svr，comet svr 内部会遍历所有的 buckets，将消息发送到 buckets 内指定房间里的每个 user 。
			_, err := c.client.BroadcastRoom(context.Background(), &comet.BroadcastRoomReq{
				RoomID: roomArg.RoomID, // 房间号
				Proto:  roomArg.Proto,
			})
			if err != nil {
				log.Errorf("c.client.BroadcastRoom(%s, reply) serverId:%s error(%v)", roomArg, c.serverID, err)
			}

		// 3. 单人推送
		case pushArg := <-pushChan:

			// 发送单房间广播消息给 comet svr，comet svr 内会根据 user 找到他归属的 bucket，进而在该 Bucket 中找出 user 对应的 Channel，然后塞进去。
			_, err := c.client.PushMsg(context.Background(), &comet.PushMsgReq{
				Keys:    pushArg.Keys,		// user keys
				Proto:   pushArg.Proto,   	// 消息体
				ProtoOp: pushArg.ProtoOp,  	// 房间号
			})
			if err != nil {
				log.Errorf("c.client.PushMsg(%s, reply) serverId:%s error(%v)", pushArg, c.serverID, err)
			}

		// 4. 退出信号
		case <-c.ctx.Done():
			return
		}
	}
}

// Close close the resouces.
// 关闭其他正在执行的 goroutine（很像没用到）。
func (c *Comet) Close() (err error) {
	finish := make(chan bool)
	go func() {
		for {
			n := len(c.broadcastChan)
			for _, ch := range c.pushChan {
				n += len(ch)
			}
			for _, ch := range c.roomChan {
				n += len(ch)
			}
			if n == 0 {
				finish <- true
				return
			}
			time.Sleep(time.Second)
		}
	}()
	select {
	case <-finish:
		log.Info("close comet finish")
	case <-time.After(5 * time.Second):
		err = fmt.Errorf("close comet(server:%s push:%d room:%d broadcast:%d) timeout", c.serverID, len(c.pushChan), len(c.roomChan), len(c.broadcastChan))
	}
	c.cancel()
	return
}
