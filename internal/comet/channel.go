package comet

import (
	"sync"

	"github.com/blastbao/goim/api/comet/grpc"
	"github.com/blastbao/goim/pkg/bufio"
)

// Channel used by message pusher send msg to write goroutine.

// Channel: 维护一个长连接，用于推送消息给 user ，它还记录了 user 在 room 里的身份识别信息 。
//
//
// 一个 channel 只能属于一个 Room。

type Channel struct {
	Room     *Room              // user 归属房间

	CliProto Ring               // cliProto 是一个 Ring Buffer，保存 Room 广播或是 client 直接发送过来的消息体


	signal   chan *grpc.Proto   // 接收消息的 chan，通过此 chan 接收 Job service 递送过来的信号，dispatcher goroutine 会监听此信号并处理。



	Writer   bufio.Writer       // 客户端连接 conn 的 写 封装
	Reader   bufio.Reader       // 客户端连接 conn 的 读 封装

	Next     *Channel 			// 双向链表 rlink
	Prev     *Channel			// 双向链表 llink

	Mid      int64				// user id
	Key      string 			// user 在 logic service 的 key
	IP       string 			// user ip


	watchOps map[int32]struct{} // user 只接收哪个房间 roomid 发來的信息

	mutex    sync.RWMutex 		// 读写锁
}

// NewChannel new a channel.
func NewChannel(cli, svr int) *Channel {
	c := new(Channel)
	c.CliProto.Init(cli)
	c.signal = make(chan *grpc.Proto, svr) 	// 接收 server 信号的缓冲区大小
	c.watchOps = make(map[int32]struct{})
	return c
}

// Watch watch a operation.
// 设置 user 关注的房间 IDs
func (c *Channel) Watch(accepts ...int32) {
	c.mutex.Lock()
	for _, op := range accepts {
		c.watchOps[op] = struct{}{}
	}
	c.mutex.Unlock()
}

// UnWatch unwatch an operation
// 移除 user 关注房间 IDs
func (c *Channel) UnWatch(accepts ...int32) {
	c.mutex.Lock()
	for _, op := range accepts {
		delete(c.watchOps, op)
	}
	c.mutex.Unlock()
}

// NeedPush verify if in watch.
// 判断 user 能否接收来自房间 op 的信息
func (c *Channel) NeedPush(op int32) bool {
	c.mutex.RLock()
	if _, ok := c.watchOps[op]; ok {
		c.mutex.RUnlock()
		return true
	}
	c.mutex.RUnlock()
	return false
}

// Push server push message.
// 给当前 user 推送消息
func (c *Channel) Push(p *grpc.Proto) (err error) {
	// 当发送信息速度大于消费速度则 c.signal 管道会阻塞，使用 select 方式来避免阻塞，
	// 但此时会有信息丢失的风险，应该报错给调用方，让它酌情重试。
	// 可以提高 c.signal 缓存大小来降低频率，但会耗费内存。
	select {
	case c.signal <- p:
	default:
	}
	return
}

// Ready check the channel ready or close?
func (c *Channel) Ready() *grpc.Proto {
	return <-c.signal
}

// Signal send signal to the channel, protocol ready.
func (c *Channel) Signal() {
	c.signal <- grpc.ProtoReady
}

// Close close the channel.
func (c *Channel) Close() {
	c.signal <- grpc.ProtoFinish
}
