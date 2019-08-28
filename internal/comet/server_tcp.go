package comet

import (
	"context"
	"io"
	"net"
	"strings"
	"time"

	"github.com/blastbao/goim/api/comet/grpc"
	"github.com/blastbao/goim/internal/comet/conf"
	"github.com/blastbao/goim/pkg/bufio"
	"github.com/blastbao/goim/pkg/bytes"
	xtime "github.com/blastbao/goim/pkg/time"
	log "github.com/golang/glog"
)

const (
	maxInt = 1<<31 - 1
)

// InitTCP listen all tcp.bind and start accept connections.
func InitTCP(server *Server, addrs []string, accept int) (err error) {
	var (
		bind     string
		listener *net.TCPListener
		addr     *net.TCPAddr
	)

	// 启动多个地址上的 TCP 监听
	for _, bind = range addrs {

		if addr, err = net.ResolveTCPAddr("tcp", bind); err != nil {
			log.Errorf("net.ResolveTCPAddr(tcp, %s) error(%v)", bind, err)
			return
		}

		if listener, err = net.ListenTCP("tcp", addr); err != nil {
			log.Errorf("net.ListenTCP(tcp, %s) error(%v)", bind, err)
			return
		}

		log.Infof("start tcp listen: %s", bind)

		// split N core accept
		for i := 0; i < accept; i++ {
			go acceptTCP(server, listener)
		}

	}

	return
}

// Accept accepts connections on the listener and serves requests for each incoming connection.
// Accept blocks;
// the caller typically invokes it in a go statement.
//
//
// acceptTCP() 函数负责监听端口上的连接请求，创建已连接对象 conn，然后交给 serveTCP() 处理。
//
func acceptTCP(server *Server, lis *net.TCPListener) {
	var (
		conn *net.TCPConn
		err  error
		r    int
	)

	for {

		// 循环 accept
		if conn, err = lis.AcceptTCP(); err != nil {
			// if listener close then return
			log.Errorf("listener.Accept(\"%s\") error(%v)", lis.Addr().String(), err)
			return
		}

		// KeepAlive
		if err = conn.SetKeepAlive(server.c.TCP.KeepAlive); err != nil {
			log.Errorf("conn.SetKeepAlive() error(%v)", err)
			return
		}
		// 设置读缓冲大小
		if err = conn.SetReadBuffer(server.c.TCP.Rcvbuf); err != nil {
			log.Errorf("conn.SetReadBuffer() error(%v)", err)
			return
		}
		// 设置写缓冲大小
		if err = conn.SetWriteBuffer(server.c.TCP.Sndbuf); err != nil {
			log.Errorf("conn.SetWriteBuffer() error(%v)", err)
			return
		}


		//
		go serveTCP(server, conn, r)

		// 循环次数 +1，这里 r 可被认为是 client 临时性 ID
		if r++; r == maxInt {
			r = 0
		}
	}
}

func serveTCP(s *Server, conn *net.TCPConn, r int) {
	var (

		// timer
		tr = s.round.Timer(r)

		// Reader Buffer
		rp = s.round.Reader(r)
		// Writer Buffer
		wp = s.round.Writer(r)

		// ip addr
		lAddr = conn.LocalAddr().String()  	// 服务端 ip:port
		rAddr = conn.RemoteAddr().String()  // 客户端 ip:port
	)


	if conf.Conf.Debug {
		log.Infof("start tcp serve \"%s\" with \"%s\"", lAddr, rAddr)
	}


	s.ServeTCP(conn, rp, wp, tr)
}





// ServeTCP serve a tcp connection.
//
// ServeTCP() 函数负责从连接 conn 中读取 client 发来的数据，写入到从 ring buffer 中，然后触发 ch.signal 通知 dispatchTCP() 协程去取消息。
//
//
// 1. 初始化 I/O 对象，定时器 ...
// 2. 鉴权
// 3. 启动 dispatchTCP() 协程异步处理消息
// 4. 从 conn 中读取消息内容写入到 ring buffer 中，并通过 ch.signal 通知 dispatchTCP() 协程去取消息
// 5. ...
func (s *Server) ServeTCP(conn *net.TCPConn, rp, wp *bytes.Pool, tr *xtime.Timer) {




	var (
		err     error



		// 房间 id
		rid     string

		// tcp 连接的 tag ，可以用于设置消息推送条件
		accepts []int32


		hb      time.Duration // heartbeat interval
		white   bool
		p       *grpc.Proto
		b       *Bucket
		trd     *xtime.TimerData
		lastHb  = time.Now()
		rb      = rp.Get()
		wb      = wp.Get()


		ch      = NewChannel(s.c.Protocol.CliProto, s.c.Protocol.SvrProto)


		rr      = &ch.Reader
		wr      = &ch.Writer
	)


	// 初始化读写 I/O 对象
	ch.Reader.ResetBuffer(conn, rb.Bytes())
	ch.Writer.ResetBuffer(conn, wb.Bytes())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()


	// handshake

	step := 0
	// 添加超时定时器，控制握手超时: 超时会打印日志和关闭连接
	trd = tr.Add(time.Duration(s.c.Protocol.HandshakeTimeout), func() {
		conn.Close()
		log.Errorf("key: %s remoteIP: %s step: %d tcp handshake timeout", ch.Key, conn.RemoteAddr().String(), step)
	})
	// 获取客户端 IP
	ch.IP, _, _ = net.SplitHostPort(conn.RemoteAddr().String())

	// must not setadv, only used in auth
	// 登陆验证所用的消息 p 不需要被其他地方读写，所以在读取&处理完该消息，并没有调用 SetAdv()，所以写游标还是 0 。
	step = 1

	// 1. 取出一个待写入数据的对象 p ，此时写游标为 0 。
	if p, err = ch.CliProto.Set(); err == nil {

		// 2. 调用 authTCP() 方法，从 conn 中读取用户鉴权信息写入到 p，然后调用 logicSvr 进行鉴权。
		if ch.Mid, ch.Key, rid, accepts, hb, err = s.authTCP(ctx, rr, wr, p); err == nil {

			// 3. 鉴权通过，保存用户登陆信息...
			ch.Watch(accepts...)
			b = s.Bucket(ch.Key)
			err = b.Put(rid, ch)
			if conf.Conf.Debug {
				log.Infof("tcp connnected key:%s mid:%d proto:%+v", ch.Key, ch.Mid, p)
			}

		}

	}

	step = 2
	// 鉴权失败
	if err != nil {
		// 关闭连接
		conn.Close()
		// 回收读写 buffer
		rp.Put(rb)
		wp.Put(wb)
		// 删除定时器
		tr.Del(trd)
		log.Errorf("key: %s handshake failed error(%v)", ch.Key, err)
		return
	}

	// 重置超时定时器，控制心跳超时: 超时会打印日志和关闭连接
	trd.Key = ch.Key
	tr.Set(trd, hb)

	// 白名单？
	white = whitelist.Contains(ch.Mid)
	if white {
		whitelist.Printf("key: %s[%s] auth\n", ch.Key, rid)
	}


	step = 3

	// hanshake ok start dispatch goroutine

	// 4. 开启一个发送 goroutine
	go s.dispatchTCP(conn, wr, wp, wb, ch)


	// 返回 [10 min, 30 min] 之间的一个随机值，被用作 comet - logic 之间上报 user 心跳的时间间隔
	serverHeartbeat := s.RandServerHearbeat()
	for {

		// 5. 取出一个待写入数据的消息对象 p ，这时候写游标仍为 0 ，
		// 因为上面登陆验证所用的 p 不需要被其他地方读写，
		// 所以在登陆验证完后，并没有调用 SetAdv()，所以此时写游标还是 0 。
		if p, err = ch.CliProto.Set(); err != nil {
			break
		}

		if white {
			whitelist.Printf("key: %s start read proto\n", ch.Key)
		}

		// 6. 从 conn 中读取消息内容写入 p 中
		if err = p.ReadTCP(rr); err != nil {
			break
		}

		if white {
			whitelist.Printf("key: %s read proto:%v\n", ch.Key, p)
		}


		// 7. 消息处理

		// 7.1 心跳消息

		// comet 有心跳机制维护 user 连接状态，对 logic 来说也需要知道哪个 user 在线，
		// 目前 user 心跳上报到 comet 之后，并不需要每次收到 user 心跳就转发给 logic ，而是增大了上报间隔。

		if p.Op == grpc.OpHeartbeat {

			// (1) 重置心跳超时定时器
			tr.Set(trd, hb)

			// (2) 构造心跳回复消息
			p.Op = grpc.OpHeartbeatReply
			p.Body = nil

			// NOTE: send server heartbeat for a long time
			// (3) 发送心跳消息
			if now := time.Now(); now.Sub(lastHb) > serverHeartbeat {
				if err1 := s.Heartbeat(ctx, ch.Mid, ch.Key); err1 == nil {
					lastHb = now
				}
			}
			if conf.Conf.Debug {
				log.Infof("tcp heartbeat receive key:%s, mid:%d", ch.Key, ch.Mid)
			}
			step++

		// 7.2 其它消息
		} else {
			// 根据不同的消息类型进行不同处理
			if err = s.Operate(ctx, p, ch, b); err != nil {
				break
			}
		}

		if white {
			whitelist.Printf("key: %s process proto:%v\n", ch.Key, p)
		}

		// 8. 消息处理完，调用 SetAdv() 使唤醒缓存写游标 +1
		ch.CliProto.SetAdv()

		// 9. 有新消息到达，触发监听者来读取
		ch.Signal()
		if white {
			whitelist.Printf("key: %s signal\n", ch.Key)
		}
	}


	if white {
		whitelist.Printf("key: %s server tcp error(%v)\n", ch.Key, err)
	}


	if err != nil && err != io.EOF && !strings.Contains(err.Error(), "closed") {
		log.Errorf("key: %s server tcp failed error(%v)", ch.Key, err)
	}

	// 如果某 user 连接 conn 有异常或是 server 要踢人，则:
	// 1. 从 Bucket 移除 user Channel ，这样 Bucket 内的 Channel 都是活跃的
	// 2. 移除心跳任务
	// 3. 回收读 Buffer，不回收写的 Buffer 是因为 Channel close 后 dispatchTCP 会被通知到并回收写 Buffer
	// 4. 关闭 conn 连接
	// 5. 关闭 channel 消息通道
	// 6. 通知 logic 某人下线了

	b.Del(ch)     	// 1
	tr.Del(trd)		// 2
	rp.Put(rb)		// 3
	conn.Close()	// 4
	ch.Close()		// 5

	// 6
	if err = s.Disconnect(ctx, ch.Mid, ch.Key); err != nil {
		log.Errorf("key: %s mid: %d operator do disconnect error(%v)", ch.Key, ch.Mid, err)
	}

	if white {
		whitelist.Printf("key: %s mid: %d disconnect error(%v)\n", ch.Key, ch.Mid, err)
	}
	if conf.Conf.Debug {
		log.Infof("tcp disconnected key: %s mid: %d", ch.Key, ch.Mid)
	}
}








// dispatch accepts connections on the listener and serves requests for each incoming connection.
// dispatch blocks;
// the caller typically invokes it in a go statement.
//
// dispatchTCP() 函数监听 ch.signal 信号，然后从 ring buffer 中取数据进行处理。
//
// 1. 不断地、阻塞式地从 ch.signal 中读取消息 p 。
// 2. 如果 p 是 ProtoFinish 类型，就关闭连接 conn 。
// 3. 如果 p 是 ProtoReady 类型，意味着有 client 发来消息，需要去消息环形缓存中取出一个消息，进行处理和回复。
// 4. 如果 p 是 其它 类型，....
//
func (s *Server) dispatchTCP(conn *net.TCPConn, wr *bufio.Writer, wp *bytes.Pool, wb *bytes.Buffer, ch *Channel) {


	var (
		err    error
		finish bool
		online int32
		white  = whitelist.Contains(ch.Mid)
	)


	if conf.Conf.Debug {
		log.Infof("key: %s start dispatch tcp goroutine", ch.Key)
	}


	// For Loop
	for {

		if white {
			whitelist.Printf("key: %s wait proto ready\n", ch.Key)
		}

		// 1. 阻塞式的从 ch.signal 中读取消息
		var p = ch.Ready()
		if white {
			whitelist.Printf("key: %s proto ready\n", ch.Key)
		}
		if conf.Conf.Debug {
			log.Infof("key:%s dispatch msg:%v", ch.Key, *p)
		}


		switch p {


		case grpc.ProtoFinish:

			if white {
				whitelist.Printf("key: %s receive proto finish\n", ch.Key)
			}

			if conf.Conf.Debug {
				log.Infof("key: %s wakeup exit dispatch goroutine", ch.Key)
			}

			finish = true
			goto failed


		// 有 client 发来消息
		case grpc.ProtoReady:
			// fetch message from svrbox(client send)
			for {

				// 从 Ring buffer 中读取一个消息 p
				if p, err = ch.CliProto.Get(); err != nil {
					break
				}

				if white {
					whitelist.Printf("key: %s start write client proto%v\n", ch.Key, p)
				}

				// 如果是心跳回复消息，在写回给客户端时，附带上当前房间内的用户总数
				if p.Op == grpc.OpHeartbeatReply {
					if ch.Room != nil {
						online = ch.Room.OnlineNum()
					}
					// 把消息 p + onlineNum 写入 conn 中，发送给客户端
					if err = p.WriteTCPHeart(wr, online); err != nil {
						goto failed
					}
				} else {
					// 把消息 p 写入 conn 中，发送给客户端
					if err = p.WriteTCP(wr); err != nil {
						goto failed
					}
				}

				if white {
					whitelist.Printf("key: %s write client proto%v\n", ch.Key, p)
				}

				// 将内存交由 GC 回收
				p.Body = nil // avoid memory leak

				// 消息读取后，调用 GetAdv() 使读游标 +1
				ch.CliProto.GetAdv()
			}


		default:

			if white {
				whitelist.Printf("key: %s start write server proto%v\n", ch.Key, p)
			}

			// server send
			if err = p.WriteTCP(wr); err != nil {
				goto failed
			}

			if white {
				whitelist.Printf("key: %s write server proto%v\n", ch.Key, p)
			}

			if conf.Conf.Debug {
				log.Infof("tcp sent a message key:%s mid:%d proto:%+v", ch.Key, ch.Mid, p)
			}

		}


		if white {
			whitelist.Printf("key: %s start flush \n", ch.Key)
		}


		// only hungry flush response
		if err = wr.Flush(); err != nil {
			break
		}

		if white {
			whitelist.Printf("key: %s flush\n", ch.Key)
		}

	}
failed:


	if white {
		whitelist.Printf("key: %s dispatch tcp error(%v)\n", ch.Key, err)
	}


	if err != nil {
		log.Errorf("key: %s dispatch tcp error(%v)", ch.Key, err)
	}
	conn.Close()
	wp.Put(wb)


	// must ensure all channel message discard, for reader won't blocking Signal
	for !finish {
		finish = (ch.Ready() == grpc.ProtoFinish)
	}


	if conf.Conf.Debug {
		log.Infof("key: %s dispatch goroutine exit", ch.Key)
	}


}

// auth for goim handshake with client, use rsa & aes.
func (s *Server) authTCP(ctx context.Context, rr *bufio.Reader, wr *bufio.Writer, p *grpc.Proto) (mid int64, key, rid string, accepts []int32, hb time.Duration, err error) {

	// 1. 从 conn 中读取第一个 OpAuth 消息
	for {
		// 读取一个完整的消息 p
		if err = p.ReadTCP(rr); err != nil {
			return
		}
		// 检查消息是否是 OpAuth 类型，如果不是就 log, skip and continue 。
		if p.Op == grpc.OpAuth {
			break
		} else {
			log.Errorf("tcp request operation(%d) not auth", p.Op)
		}
	}

	// 2. 鉴权调用
	if mid, key, rid, accepts, hb, err = s.Connect(ctx, p, ""); err != nil {
		log.Errorf("authTCP.Connect(key:%v).err(%v)", key, err)
		return
	}

	// 3. 鉴权消息回复
	p.Op = grpc.OpAuthReply
	p.Body = nil
	if err = p.WriteTCP(wr); err != nil {
		log.Errorf("authTCP.WriteTCP(key:%v).err(%v)", key, err)
		return
	}
	err = wr.Flush()

	return
}
