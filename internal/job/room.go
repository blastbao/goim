package job

import (
	"errors"
	"time"

	comet "github.com/blastbao/goim/api/comet/grpc"
	"github.com/blastbao/goim/internal/job/conf"
	"github.com/blastbao/goim/pkg/bytes"
	log "github.com/golang/glog"
)

var (
	// ErrComet commet error.
	ErrComet = errors.New("comet rpc is not available")
	// ErrCometFull comet chan full.
	ErrCometFull = errors.New("comet proto chan full")
	// ErrRoomFull room chan full.
	ErrRoomFull = errors.New("room proto chan full")

	// 特殊消息: 当 pushproc goroutine 收到此消息，会 flush 缓存的消息，一次性发送到 commet 的消息管道中。
	roomReadyProto = new(comet.Proto)
)



// 【重要】Room 结构体仅仅是封装 `单房间广播消息` 的缓存和聚合发送的逻辑，起个这么抽象的名太容易混淆了。

// Room room.
type Room struct {
	c     *conf.Room
	job   *Job					//
	id    string 				// 房间id
	proto chan *comet.Proto 	// 发送消息的 chan
}

// NewRoom new a room struct, store channel room info.
func NewRoom(job *Job, id string, c *conf.Room) (r *Room) {

	r = &Room{
		c:     c,
		id:    id,
		job:   job,
		proto: make(chan *comet.Proto, c.Batch*2),
	}

	// 启动 goroutine 负责缓存 `单房间广播` 消息的接收、缓存、聚合、发送给 comet 管道。
	go r.pushproc(c.Batch, time.Duration(c.Signal))
	return
}

// Push push msg to the room, if chan full discard it.
func (r *Room) Push(op int32, msg []byte) (err error) {

	// 1. 构造消息
	var p = &comet.Proto{
		Ver:  1, 	// 版本号
		Op:   op,	// 操作类型
		Body: msg,  // 消息体
	}

	// 2. 推送消息到 r.proto 管道，满则报错
	select {
	case r.proto <- p:
	default:
		err = ErrRoomFull
	}

	return
}

// pushproc merge proto and push msgs in batch.
//
// 功能:
//  负责单房间广播消息的接收、缓存、聚合、发送给 comet 管道。
//
// 参数说明:
// 	batch: 控制缓存超过多少条则执行推送。
// 	sigTime: 控制缓存超过多少时间则执行推送。
//
func (r *Room) pushproc(batch int, sigTime time.Duration) {


	var (
		// 缓存消息条数
		n    int
		// 记录缓存消息 flush 后收到第一条消息的时间，用于控制消息在缓存中存留的最大时间，超过则会被 flush，确保及时性。
		last time.Time
		// 保存当前接收消息的临时消息结构
		p    *comet.Proto
		// 消息缓存
		buf  = bytes.NewWriterSize(int(comet.MaxBodySize))
	)

	log.Infof("start room:%s goroutine", r.id)


	// 创建定时器，控制在 sigTime 之后会执行 func() 函数，将 roomReadyProto 消息推入 r.proto 管道，
	// `roomReadyProto` 的作用是主动触发将已缓存的消息 flush 发送给 comet svr 。
	td := time.AfterFunc(sigTime, func() {
		select {
		case r.proto <- roomReadyProto:
		default:
		}
	})
	defer td.Stop()

	for {

		// 监听 r.proto 管道，如果管道被关闭 (nil) 则退出监听循环 。

		if p = <-r.proto; p == nil {
			break // exit
		} else if p != roomReadyProto {

			// 如果消息不为 `roomReadyProto` ，则不需要强制 flush ，把当前消息写入到 buf 中，忽略写入 error
			p.WriteTo(buf)

			// 增加缓存消息条数，如果是缓存清空后的第一条消息，需要重置定时器，设置多久后再次执行推送。
			if n++; n == 1 {
				last = time.Now() 	// 更新缓存清空后收到第一条消息的时间
				td.Reset(sigTime) 	// 重置定时器
				continue			// 继续取下一条消息

			// 检查当前消息数目是否达到 flush 阈值，如果达到则退出 if-else 语句块，走到下面 flush 流程。
			} else if n < batch {
				// 检查自缓存清空后收到第一条消息，至收到本条消息之间花费的时间，如果超过阈值 sigTime 则退出 if-else 语句块，走到下面 flush 流程，否则 continue 。
				if sigTime > time.Since(last) {
					continue
				}
			}

		} else {
			// 至此，意味着收到的是 `roomReadyProto` 消息，需要 flush ，flush 前检查一下缓存的消息条数是否为 0 。
			if n == 0 {
				break
			}
		}

		// 发送单房间广播聚合消息给 comet svr 。
		_ = r.job.broadcastRoomRawBytes(r.id, buf.Buffer())

		// 重置（清空） buf 消息缓存。
		// TODO use reset buffer
		// after push to room channel, renew a buffer, let old buffer gc
		buf = bytes.NewWriterSize(buf.Size())

		// 重置已缓存消息数目。
		n = 0

		// 每发送完一次聚合消息，就重置 Idle 定时器，如果很久都没有再次发送聚合消息，Idle 定时器到达时，会发送 `roomReadyProto` 消息，以触发主动 flush 。
		if r.c.Idle != 0 {
			td.Reset(time.Duration(r.c.Idle))
		} else {
			td.Reset(time.Minute)
		}
	}

	// 至此，已经退出 for 循环，停止接收新消息，退出条件为:
	//
	// （1）消息管道 r.proto 被关闭
	// （2) 收到 `roomReadyProto` 消息时缓存的消息条数为 0
	//
	// 这意味着本 goroutine 即将退出，因此本 goroutine 负责的 room 上的广播聚合消息也不再接收和推送，
	// 这里的操作是将 room 从 Job 里删除。
	r.job.delRoom(r.id)

	log.Infof("room:%s goroutine exit", r.id)
}

func (j *Job) delRoom(roomID string) {
	j.roomsMutex.Lock()
	delete(j.rooms, roomID)
	j.roomsMutex.Unlock()
}

func (j *Job) getRoom(roomID string) *Room {

	// 判断 roomID 是否存在于本 Job 中
	j.roomsMutex.RLock()
	room, ok := j.rooms[roomID]
	j.roomsMutex.RUnlock()

	if !ok {
		// 双检锁
		j.roomsMutex.Lock()
		if room, ok = j.rooms[roomID]; !ok {
			// 若 roomID 不存在于本 Job 中，则新建 room 并插入到 Job 中。
			room = NewRoom(j, roomID, j.c.Room)
			j.rooms[roomID] = room
		}
		j.roomsMutex.Unlock()
		log.Infof("new a room:%s active:%d", roomID, len(j.rooms))
	}

	// 返回 room 对象
	return room
}
