package comet

import (
	"github.com/blastbao/goim/api/comet/grpc"
	"github.com/blastbao/goim/internal/comet/conf"
	"github.com/blastbao/goim/internal/comet/errors"
	log "github.com/golang/glog"
)

// Ring ring proto buffer.
type Ring struct {

	// read
	rp   uint64 		// 读位置
	num  uint64 		// 总长度
	mask uint64 		// 掩码 --- 用于计数 +1 超出范围时重置
	// TODO split cacheline, many cpu cache line size is 64
	// pad [40]byte

	// write
	wp   uint64 		// 写位置
	data []grpc.Proto  	// 对象数组

}


// NewRing new a ring buffer.
func NewRing(num int) *Ring {
	r := new(Ring)
	r.init(uint64(num))
	return r
}

// Init init ring.
func (r *Ring) Init(num int) {
	r.init(uint64(num))
}

// 初始化对象数组长度为2的n次方
func (r *Ring) init(num uint64) {

	// `num&(num-1)` 的作用是去掉 `num` 的二进制表示中的最后一个 1 ，如下:
	//
	//	  1000 0001 0000 0000
	//	& 1000 0000 1111 1111
	//	  ==== ==== ==== ====
	//	= 1000 0000 0000 0000
	//
	// 若 `num&(num-1) == 0` ，则 `num` 是 0 或者是 2 的幂。
	//
	//	  1000 0000 0000 0000
	//	&  111 1111 1111 1111
	//	  ==== ==== ==== ====
	//  = 0000 0000 0000 0000


	// 所以下面这段代码的含义是，不断消除 num 末尾的 `1`，
	// 这样最终得到的是只剩一个最高位的 `1` 的数字，它是 2 的幂。
	// 然后让它 * 2 ，这样就能得到大于 num 的最小的 2 的幂的整数。



	// 计算大于 num 的最小的 2 的幂的整数 2^N。
	if num&(num-1) != 0 {
		// 不断消除 num 末尾的 `1`，最终只剩一个最高位的 `1`
		for num&(num-1) != 0 {
			num &= (num - 1)
		}
		// 乘以 2
		num = num << 1
	}

	// 创建 num 大小的消息数组
	r.data = make([]grpc.Proto, num)
	// 设置 Ring 的容量为 num
	r.num = num
	// 掩码，用于溢出时回环
	r.mask = r.num - 1
}

// Get get a proto from ring.

func (r *Ring) Get() (proto *grpc.Proto, err error) {

	// 如果读位置 rp 等于写位置 wp，则队列为空
	if r.rp == r.wp {
		return nil, errors.ErrRingEmpty
	}

	// 队列非空，则取出 rp 位置的消息
	proto = &r.data[r.rp&r.mask]
	return
}

// GetAdv incr read index.
// 读游标 +1
func (r *Ring) GetAdv() {
	r.rp++
	if conf.Conf.Debug {
		log.Infof("ring rp: %d, idx: %d", r.rp, r.rp&r.mask)
	}
}

// Set get a proto to write.
// 取待写入对象
func (r *Ring) Set() (proto *grpc.Proto, err error) {
	// 队列满？
	if r.wp-r.rp >= r.num {
		return nil, errors.ErrRingFull
	}
	// 取出 r.wp 位置上的 proto 用来写入新消息
	proto = &r.data[r.wp&r.mask]
	return
}

// SetAdv incr write index.
// 写游标 +1
func (r *Ring) SetAdv() {
	r.wp++
	if conf.Conf.Debug {
		log.Infof("ring wp: %d, idx: %d", r.wp, r.wp&r.mask)
	}
}

// Reset reset ring.
// 重置读写游标
func (r *Ring) Reset() {
	r.rp = 0
	r.wp = 0
	// prevent pad compiler optimization
	// r.pad = [40]byte{}
}
