package comet

import (
	"context"
	"math/rand"
	"time"

	logic "github.com/blastbao/goim/api/logic/grpc"
	"github.com/blastbao/goim/internal/comet/conf"
	log "github.com/golang/glog"
	"github.com/zhenjl/cityhash"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/keepalive"
)

const (

	minServerHeartbeat = time.Minute * 10 			// 通知 logic 刷新 client 心跳状态最小心跳时间
	maxServerHeartbeat = time.Minute * 30			// 通知 logic 刷新 client 心跳状态最大心跳时间

	// grpc options
	grpcInitialWindowSize     = 1 << 24
	grpcInitialConnWindowSize = 1 << 24
	grpcMaxSendMsgSize        = 1 << 24
	grpcMaxCallMsgSize        = 1 << 24
	grpcKeepAliveTime         = time.Second * 10	// grpc 心跳包的频率
	grpcKeepAliveTimeout      = time.Second * 3		// grpc 心跳回复如果超过此时间则 close 连接
	grpcBackoffMaxDelay       = time.Second * 3		// grpc 连接失败后等待多久再开始尝试重连
)

func newLogicClient(c *conf.RPCClient) logic.LogicClient {

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.Dial))
	defer cancel()
	conn, err := grpc.DialContext(ctx, "discovery://default/goim.logic",
		[]grpc.DialOption{
			// comet 与 logic 通信不用检查安全状态
			grpc.WithInsecure(),
			// grpc - Http2 相关参数设置
			grpc.WithInitialWindowSize(grpcInitialWindowSize),
			grpc.WithInitialConnWindowSize(grpcInitialConnWindowSize),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(grpcMaxCallMsgSize)),
			grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(grpcMaxSendMsgSize)),
			grpc.WithBackoffMaxDelay(grpcBackoffMaxDelay),
			// grpc - 心跳机制参数设置
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                grpcKeepAliveTime,
				Timeout:             grpcKeepAliveTimeout,
				PermitWithoutStream: true,
			}),
			// grpc - 设置 LB 策略，需要有 服务发现 支持
			grpc.WithBalancerName(roundrobin.Name),
		}...)
	if err != nil {
		panic(err)
	}
	return logic.NewLogicClient(conn)
}

// Server is comet server.
type Server struct {
	c         *conf.Config

	round     *Round    // 负责 ReaderBuffer / WriterBuffer / Timer 的 Pool   	// accept round store
	buckets   []*Bucket // buckets 列表											// subkey bucket
	bucketIdx uint32    // bucket 总数

	serverID  string
	rpcClient logic.LogicClient
}

// NewServer returns a new Server.
func NewServer(c *conf.Config) *Server {
	s := &Server{
		c:         c,
		round:     NewRound(c),
		rpcClient: newLogicClient(c.RPCClient),
	}

	// 初始化 Bucket
	s.buckets = make([]*Bucket, c.Bucket.Size)
	s.bucketIdx = uint32(c.Bucket.Size)
	for i := 0; i < c.Bucket.Size; i++ {
		s.buckets[i] = NewBucket(c.Bucket)
	}
	s.serverID = c.Env.Host

	// 统计线上各房间的人数
	go s.onlineproc()
	return s
}

// Buckets return all buckets.
func (s *Server) Buckets() []*Bucket {
	return s.buckets
}



// Bucket get the bucket by subkey.
//
// 用 CityHash32 计算 user key 的哈希值，然后对 bucket 总数取余算得 idx ，来取出 user key 对应的 bucket。
// 这种方法能够尽可能将 user 分散到不同的 bucket 中，随着 bucket 的增加，每个 bucket 内的 user 总数是降低的，
// 这样便可以在高并发时，避免在同一个 bucket 上的频繁锁竞争。
//
// 注，一个 user key 会始终映射到相同的 bucket 里，除非发生 bucket 的扩容，这个另做讨论。
func (s *Server) Bucket(subKey string) *Bucket {
	idx := cityhash.CityHash32([]byte(subKey), uint32(len(subKey))) % s.bucketIdx
	if conf.Conf.Debug {
		log.Infof("%s hit channel bucket index: %d use cityhash", subKey, idx)
	}
	return s.buckets[idx]
}

// RandServerHearbeat rand server heartbeat.
// 返回 [10 min, 30 min] 之间的一个随机值，被用作 comet - logic 之间上报 user 心跳的时间间隔
func (s *Server) RandServerHearbeat() time.Duration {
	return (minServerHeartbeat + time.Duration(rand.Int63n(int64(maxServerHeartbeat-minServerHeartbeat))))
}

// Close close the server.
func (s *Server) Close() (err error) {
	return
}


// 统计各房间人数并发给 logic ，因为 logic 有提供 `获得某房间总在线人数` 的 http 接口。
func (s *Server) onlineproc() {
	for {

		var (
			allRoomsCount map[string]int32
			err           error
		)

		// 房间会分散在不同的 bucket， 所以需要遍历 s.buckets 来汇总全局结果
		roomCount := make(map[string]int32)
		for _, bucket := range s.buckets {
			for roomID, count := range bucket.RoomsCount() {
				roomCount[roomID] += count
			}
		}

		// 发送给 logic
		if allRoomsCount, err = s.RenewOnline(context.Background(), s.serverID, roomCount); err != nil {
			time.Sleep(time.Second)
			continue
		}

		// ？？？
		for _, bucket := range s.buckets {
			bucket.UpRoomsCount(allRoomsCount)
		}

		// 每十秒 loop 一次
		time.Sleep(time.Second * 10)
	}
}
