package comet

import (
	"sync"
	"sync/atomic"

	"github.com/blastbao/goim/api/comet/grpc"
	"github.com/blastbao/goim/internal/comet/conf"
)


// 每个 Comet 程序拥有若干个 Bucket, 可以理解为 Session Management, 保存着当前 Comet 服务于哪些 Room 和 Channel.
// 长连接具体分布在哪个 Bucket 上呢？根据 SubKey 一致性 Hash 来选择。


// Bucket is a channel holder.


// Bucket 结构体被用于维护当前消息通道和房间的信息。
// 一个 Comet Server 默认开启 1024 Bucket, 这样做的好处是减少锁 ( Bucket.cLock ) 争用，在大并发业务上尤其明显。

type Bucket struct {

	c     *conf.Bucket
	cLock sync.RWMutex        		// protect the channels for chs
	chs   map[string]*Channel 		// 维护当前 bucket 内 user 与其 Channel 的映射关系，len(chs) 即为当前 bucket 用户总数。

	rooms       map[string]*Room 	// 维护当前 bucket 内的房间信息，len(rooms) 即为当前 bucket 内房间总数。

	// 一个 Bucket 会开多个 goroutine 来并发的处理单房间的广播消息推送，每个 goroutine 监听一个消息管道。
	// 推送单房间广播消息时，可以随机发往任何管道，交由对应的 goroutine 来处理。
	// 这里为了保证负载均衡，使用了原子锁+递增方式，本质就是轮训选择一个 goroutine 。
	routines    []chan *grpc.BroadcastRoomReq

	// 用于决定由哪一个 routines 来做房间广播推送，此数字由 atomic.AddUint64 做原子递增
	routinesNum uint64

	// 记录有哪些 ip 在此 bucket ，key=ip、value=重复ip数量 。
	ipCnts map[string]int32
}

// NewBucket new a bucket struct. store the key with im channel.
func NewBucket(c *conf.Bucket) (b *Bucket) {
	b = new(Bucket)
	b.chs = make(map[string]*Channel, c.Channel)
	b.ipCnts = make(map[string]int32)
	b.c = c
	b.rooms = make(map[string]*Room, c.Room)
	b.routines = make([]chan *grpc.BroadcastRoomReq, c.RoutineAmount)

	// 循环创建 c.RoutineAmount 个单房间广播管道和对应的 goroutine，负责并发的接收和处理单房间广播消息。
	for i := uint64(0); i < c.RoutineAmount; i++ {
		// 创建一个单房间广播管道 c ，每个管道的大小为 c.RoutineSize
		c := make(chan *grpc.BroadcastRoomReq, c.RoutineSize)

		// 把 c 保存到 b.routines[] 中
		b.routines[i] = c

		// 为每个管道 c 创建一个 goroutine，主要处理:
		// 	(1) 监听单房间广播消息管道: 忽略不属于当前 bucket 的房间广播消息。
		// 	(2) 房间内广播: 把消息 p 逐个地发送给房间内的所有 Channel。
		go b.roomproc(c)
	}
	return
}

// ChannelCount channel count in the bucket
// 当前 bucket 内用户总数。
func (b *Bucket) ChannelCount() int {
	return len(b.chs)
}

// RoomCount room count in the bucket
// 当前 bucket 内房间总数。
func (b *Bucket) RoomCount() int {
	return len(b.rooms)
}

// RoomsCount get all room id where online number > 0.
// 获取每个房间内的用户数目。
func (b *Bucket) RoomsCount() (res map[string]int32) {
	var (
		roomID string
		room   *Room
	)
	b.cLock.RLock()
	res = make(map[string]int32)
	for roomID, room = range b.rooms {
		if room.Online > 0 {
			res[roomID] = room.Online // room.Online 值为房间的在线用户数目
		}
	}
	b.cLock.RUnlock()
	return
}

// ChangeRoom change ro room
// user 更换房间。
func (b *Bucket) ChangeRoom(nrid string, ch *Channel) (err error) {
	var (
		nroom *Room
		ok    bool
		oroom = ch.Room
	)
	// change to no room
	if nrid == "" {
		if oroom != nil && oroom.Del(ch) {
			b.DelRoom(oroom)
		}
		ch.Room = nil
		return
	}
	b.cLock.Lock()
	if nroom, ok = b.rooms[nrid]; !ok {
		nroom = NewRoom(nrid)
		b.rooms[nrid] = nroom
	}
	b.cLock.Unlock()
	if oroom != nil && oroom.Del(ch) {
		b.DelRoom(oroom)
	}
	
	if err = nroom.Put(ch); err != nil {
		return
	}
	ch.Room = nroom
	return
}

// Put put a channel according with sub key.

// 将 user Channel 分配到某个房间，
//
//
//
//
// 总共会有三种结构互相會有三種結構互相關聯Bucket,Room,Channel
// 假設Room id = A , Channel key = B
// 1. Bucket put Channel(B)
// 2. Bucket put Room(A)
// 3. Channel(B) Room 對應到 Room(A)
// 4. Room(A) Channel put Channel(B)
// =============================================
// 		Bucket
//  		- []Room    			- []Channel
//			|						|
//			↓						↓
//		 Room(A) ←----------	 Channel(B) ←-|
//			- Channel		|-------- Room     |
//          |----------------------------------|
//

func (b *Bucket) Put(rid string, ch *Channel) (err error) {

	var (
		room *Room
		ok   bool
	)

	b.cLock.Lock()
	// b.chs[] 中维护了当前 bucket 内 user 与 Channel 的映射关系。
	// 这里先判断 user 是否已经存在于 b.chs[]，若存在就关闭之前的 channel 。
	if dch := b.chs[ch.Key]; dch != nil {
		dch.Close()
	}

	// 保存新的 <user, channel> 映射关系。
	b.chs[ch.Key] = ch

	// 如果 rid 不为空，就获取对应 room 保存到 ch.Room 上。
	if rid != "" {
		// 检查 roomID 是否已经存在于本 bucket，若不存在就 new 一个放进去
		if room, ok = b.rooms[rid]; !ok {
			room = NewRoom(rid)
			b.rooms[rid] = room
		}
		// 把 room 设置到 ch.Room 上
		ch.Room = room
	}

	// 更新来自 ch.IP 的在线用户数目
	b.ipCnts[ch.IP]++
	b.cLock.Unlock()

	// 保存 room -> user channel 的映射关系，这样 room 房间的广播会发送到当前 user channel
	if room != nil {
		err = room.Put(ch)
	}

	return
}

// Del delete the channel by sub key.
//
// 刪除某个 user Channel
// 	1. Bucket 刪除 []Channel 对应的 Channel
// 	2. Bucket 刪除 []Room 內对应的 Channel
// 	3. 如果 Room 沒人则 Bucket 刪除对应 Room

func (b *Bucket) Del(dch *Channel) {

	var (
		ok   bool
		ch   *Channel
		room *Room
	)

	b.cLock.Lock()

	// 当前 ch 存在
	if ch, ok = b.chs[dch.Key]; ok {

		// 取出归属 room
		room = ch.Room

		// 删除 channel
		if ch == dch {
			delete(b.chs, ch.Key)
		}

		// 更新 IP 计数器
		if b.ipCnts[ch.IP] > 1 {
			b.ipCnts[ch.IP]--
		} else {
			delete(b.ipCnts, ch.IP)
		}
	}
	b.cLock.Unlock()

	if room != nil && room.Del(ch) {
		// if empty room, must delete from bucket
		b.DelRoom(room)
	}
}

// Channel get a channel by sub key.
// 根据 user key 获取对应 Channel
func (b *Bucket) Channel(key string) (ch *Channel) {
	b.cLock.RLock()
	ch = b.chs[key]
	b.cLock.RUnlock()
	return
}

// Broadcast push msgs to all channels in the bucket.
// 把消息 p 推送到对本 Bucket 内所有关注房间 op 的 channels 中。
func (b *Bucket) Broadcast(p *grpc.Proto, op int32) {
	var ch *Channel
	b.cLock.RLock()
	// 遍历 Bucket 内的所有 channels
	for _, ch = range b.chs {
		// 检查 ch 是否需要接收房间 op 发来的信息
		if !ch.NeedPush(op) {
			continue
		}
		// 发送消息 p 给 ch
		_ = ch.Push(p)
	}
	b.cLock.RUnlock()
}

// Room get a room by roomid.
// 根据房间 ID 获取房间对象。
func (b *Bucket) Room(rid string) (room *Room) {
	b.cLock.RLock()
	room = b.rooms[rid]
	b.cLock.RUnlock()
	return
}

// DelRoom delete a room by roomid.
// 从 b.rooms 中删除 room.ID 并执行关闭 room.Close()。
func (b *Bucket) DelRoom(room *Room) {
	b.cLock.Lock()
	delete(b.rooms, room.ID)
	b.cLock.Unlock()
	room.Close()
}

// BroadcastRoom broadcast a message to specified room
//
// 一个 Bucket 会开多个 goroutine 来并发的做单房间的广播消息推送，每个 goroutine 监听一个管道。
// 推送单房间广播消息时，可以随机发往任何管道，交由对应的 goroutine 来处理。
// 这里为了保证负载均衡，使用了 原子锁 + 递增 方式，本质就是轮循选择一个 goroutine 。
//
// 这里 BroadcastRoom() 函数就是简单选择一个管道，然后把消息发给它，后面 go roomproc() 中会取出并处理。
func (b *Bucket) BroadcastRoom(arg *grpc.BroadcastRoomReq) {
	// 将 routinesNum 递增 1 后对 b.c.RoutineAmount 取余，得到 idx ，则 b.routines[idx] 被用于广播。
	num := atomic.AddUint64(&b.routinesNum, 1) % b.c.RoutineAmount
	b.routines[num] <- arg
}

// Rooms get all room id where online number > 0.
func (b *Bucket) Rooms() (res map[string]struct{}) {
	var (
		roomID string
		room   *Room
	)
	res = make(map[string]struct{})
	b.cLock.RLock()
	for roomID, room = range b.rooms {
		if room.Online > 0 {
			res[roomID] = struct{}{}
		}
	}
	b.cLock.RUnlock()
	return
}

// IPCount get ip count.
// 获取 bucket 内用户的独立 IP 总数。
func (b *Bucket) IPCount() (res map[string]struct{}) {
	var (
		ip string
	)
	b.cLock.RLock()
	res = make(map[string]struct{}, len(b.ipCnts))
	for ip = range b.ipCnts {
		res[ip] = struct{}{}
	}
	b.cLock.RUnlock()
	return
}

// UpRoomsCount update all room count
//
func (b *Bucket) UpRoomsCount(roomCountMap map[string]int32) {
	var (
		roomID string
		room   *Room
	)
	b.cLock.RLock()
	for roomID, room = range b.rooms {
		room.AllOnline = roomCountMap[roomID]
	}
	b.cLock.RUnlock()
}

// roomproc 主要功能:
// 	(1) 监听单房间广播消息管道: 忽略不属于当前 bucket 的房间广播消息。
// 	(2) 房间内广播: 把消息 p 逐个地发送给房间内的所有 Channel。
func (b *Bucket) roomproc(c chan *grpc.BroadcastRoomReq) {
	for {

		// 1. 取出单房间广播消息
		arg := <-c

		// 2. 检查房间 ID 是否存在于当前 Bucket 中，若不存在则直接忽略；
		// 	否则进行房间内广播: 把消息 p 逐个地发送给房间内的所有 Channel。
		if room := b.Room(arg.RoomID); room != nil {
			room.Push(arg.Proto)
		}
	}
}
