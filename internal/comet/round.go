package comet

import (
	"github.com/blastbao/goim/internal/comet/conf"
	"github.com/blastbao/goim/pkg/bytes"
	"github.com/blastbao/goim/pkg/time"
)





// 1. 为了优化内存使用，作者自行设计一个 bytes pool 来管理内存。
// 2. 当一个 client 连接成功后需要有一个心跳机制去维护连接可不可用，所以每个 client 需要搭配一个超时定时器，
// 	  如果有 100w+ 连接就要 100w+ time.NewTicker 定时器，为了优化这一块所以作者自行实现一个 timer。





// RoundOptions round options.
type RoundOptions struct {
	Timer        int 	// 每次要分配多少个用于 time.Timer 的 Pool
	TimerSize    int 	// 每个 time.Timer 一开始能接收的 TimerData 数量
	Reader       int 	// 每次要分配多少个用于 Reader bytes 的 Pool
	ReadBuf      int 	// 每个 Reader bytes Pool 有多少个 Buffer
	ReadBufSize  int 	// 每个 Reader bytes Pool 的 Buffer 能有多大的空间
	Writer       int	// 每次要分配多少个用于 Writer bytes 的 Pool
	WriteBuf     int	// 每个 Writer bytes Pool 有多少个 Buffer
	WriteBufSize int 	// 每个 Writer bytes Pool 的 Buffer 能有多大的空间
}

// Round userd for connection round-robin get a reader/writer/timer for split big lock.
type Round struct {
	readers []bytes.Pool 	// 管理 Reader bytes Pool
	writers []bytes.Pool 	// 管理 Writer bytes Pool
	timers  []time.Timer  	// 管理 Timer Pool，每个 time.Timer 是一个最小堆
	options RoundOptions   	// Pool 相关 config
}






// NewRound new a round struct.
func NewRound(c *conf.Config) (r *Round) {
	var i int
	r = &Round{
		options: RoundOptions{
			Reader:       c.TCP.Reader,
			ReadBuf:      c.TCP.ReadBuf,
			ReadBufSize:  c.TCP.ReadBufSize,
			Writer:       c.TCP.Writer,
			WriteBuf:     c.TCP.WriteBuf,
			WriteBufSize: c.TCP.WriteBufSize,
			Timer:        c.Protocol.Timer,
			TimerSize:    c.Protocol.TimerSize,
		},
	}

	// 依照 c *conf.Config 初始化 Round 结构体:
	// 	1. 确定初始 Reader 的 []bytes.Pool 数目，每个 bytes.Pool 包含多少个 ReadBuf，每个 ReadBuf 的 bufSize
	// 	2. 确定初始 Writer 的 []bytes.Pool 数目，每个 bytes.Pool 包含多少个 WriteBuf，每个 WriteBuf 的 bufSize
	// 	3. 确定初始的 []time.Timer 数目，每个 Timer 能容纳多少的 TimerData


	// reader
	r.readers = make([]bytes.Pool, r.options.Reader)
	for i = 0; i < r.options.Reader; i++ {
		r.readers[i].Init(r.options.ReadBuf, r.options.ReadBufSize)
	}
	// writer
	r.writers = make([]bytes.Pool, r.options.Writer)
	for i = 0; i < r.options.Writer; i++ {
		r.writers[i].Init(r.options.WriteBuf, r.options.WriteBufSize)
	}
	// timer
	r.timers = make([]time.Timer, r.options.Timer)
	for i = 0; i < r.options.Timer; i++ {
		r.timers[i].Init(r.options.TimerSize)
	}
	return
}


// Timer get a timer.
// 取 Timer Pool ，给定数字 rn 会以取余方式选择其中一个，用于分散锁竞争压力，增加并发量。
func (r *Round) Timer(rn int) *time.Timer {
	return &(r.timers[rn%r.options.Timer])
}


// Reader get a reader memory buffer.
// 取 Reader Pool ，给定数字 rn 会以取余方式选择其中一个，用于分散锁竞争压力，增加并发量。
func (r *Round) Reader(rn int) *bytes.Pool {
	return &(r.readers[rn%r.options.Reader])
}

// Writer get a writer memory buffer pool.
// 取 Writer Pool ，给定数字 rn 会以取余方式选择其中一个，用于分散锁竞争压力，增加并发量。
func (r *Round) Writer(rn int) *bytes.Pool {
	return &(r.writers[rn%r.options.Writer])
}
