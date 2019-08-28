package comet

import (
	"sync"

	"github.com/blastbao/goim/api/comet/grpc"
	"github.com/blastbao/goim/internal/comet/errors"
)

// Room is a room and store channel room info.


// Room 可以理解为房间、群组、或是一个 Group。 Room 内维护 N 个 Channel，即 N 个长连接用户。
//
// 推送的消息可以在 Room 内广播，也可以推送到指定的 Channel，广播消息会发送给房间内的所有 Channel。
//
// Room 不但要维护所属的消息通道 Channel，还要负责消息广播的合并写，即 Batch Write，
// 如果不合并写，每来一个小的消息都通过长连接写出去，系统 Syscall 调用的开销会非常大，pprof 时会看到网络 Syscall 是大头。

type Room struct {
	ID        string           	// 房间号
	rLock     sync.RWMutex 		// 锁
	next      *Channel			// next *Channel是一个双向链表，保存该房间的所有客户端的 Channel；复杂度为o(1)，效率比较高。
	drop      bool 	 			// 在线状态: 标示房间是否存活，false=存活，true=非活
	Online    int32 			// 房间的 channel 数量，即房间的在线用户的多少 // dirty read is ok
	AllOnline int32
}



// NewRoom new a room struct, store channel room info.
func NewRoom(id string) (r *Room) {
	r = new(Room)
	r.ID = id
	r.drop = false
	r.next = nil
	r.Online = 0
	return
}

// Put put channel into the room.
// 加入 Room
func (r *Room) Put(ch *Channel) (err error) {
	r.rLock.Lock()
	// 1. 房间存活
	if !r.drop {
		// 1.1 将 ch 插入到双向链表 next * Channel 的第一位置
		if r.next != nil {
			r.next.Prev = ch
		}
		ch.Next = r.next
		ch.Prev = nil
		r.next = ch // insert to header
		// 1.2 增加在线 Channel 数目
		r.Online++
	} else {
		// 2. 房间非活
		err = errors.ErrRoomDroped
	}
	r.rLock.Unlock()
	return
}

// Del delete channel from the room.
func (r *Room) Del(ch *Channel) bool {
	r.rLock.Lock()

	// 1. 从双向链表 next 中删除 ch
	if ch.Next != nil {
		// if not footer
		ch.Next.Prev = ch.Prev
	}
	if ch.Prev != nil {
		// if not header
		ch.Prev.Next = ch.Next
	} else {
		r.next = ch.Next
	}

	// 2. 递减在线 Channel 数目
	r.Online--

	// 3. 如果在线 Channel 数目为 0 ，则置 r.drop 为 true
	r.drop = (r.Online == 0)
	r.rLock.Unlock()
	return r.drop
}

// Push push msg to the room, if chan full discard it.
// 房间消息广播
func (r *Room) Push(p *grpc.Proto) {
	r.rLock.RLock()
	// 把消息 p 逐个发送给房间内的所有 Channel
	for ch := r.next; ch != nil; ch = ch.Next {
		_ = ch.Push(p)
	}
	r.rLock.RUnlock()
}

// Close close the room.
func (r *Room) Close() {
	r.rLock.RLock()
	// 逐个关闭房间内的所有 Channel
	for ch := r.next; ch != nil; ch = ch.Next {
		ch.Close()
	}
	r.rLock.RUnlock()
}

// OnlineNum the room all online.
func (r *Room) OnlineNum() int32 {
	// ?
	if r.AllOnline > 0 {
		return r.AllOnline
	}
	return r.Online
}
