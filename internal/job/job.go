package job

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/blastbao/goim/api/logic/grpc"
	"github.com/blastbao/goim/internal/job/conf"
	"github.com/bilibili/discovery/naming"
	"github.com/gogo/protobuf/proto"

	cluster "github.com/bsm/sarama-cluster"
	log "github.com/golang/glog"
)

// Job is push job.
type Job struct {
	c            *conf.Config

	// 接收 Kafka 推送消息
	consumer     *cluster.Consumer

	// 线上正在运行哪些 Comet Svrs，此集合会通过 `服务发现` 来监听服务变更。
	cometServers map[string]*Comet

	// 因为类 Room 封装了 `单房间广播消息` 的缓存和聚合发送的逻辑，所以 rooms 就是汇总了这些 Room。
	rooms      map[string]*Room

	roomsMutex sync.RWMutex
}


// New new a push job.
func New(c *conf.Config) *Job {

	j := &Job{
		c:        c,
		consumer: newKafkaSub(c.Kafka),
		rooms:    make(map[string]*Room),
	}

	// 服务发现，动态更新 cometServers 列表 。
	j.watchComet(c.Discovery)

	return j
}

func newKafkaSub(c *conf.Kafka) *cluster.Consumer {

	// Kafka 集群配置
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	// 订阅 Topic
	consumer, err := cluster.NewConsumer(c.Brokers, c.Group, []string{c.Topic}, config)
	if err != nil {
		panic(err)
	}
	return consumer
}

// Close close resounces.
func (j *Job) Close() error {
	if j.consumer != nil {
		return j.consumer.Close()
	}
	return nil
}

// Consume messages, watch signals
// 【主逻辑】接收 kafka 的消息，如果是单用户消息就发送给指定 comet，单房间广播和全局广播就发送给所有的 comet  。
func (j *Job) Consume() {
	for {
		select {

		case err := <-j.consumer.Errors():
			log.Errorf("consumer error(%v)", err)
		case n := <-j.consumer.Notifications():
			log.Infof("consumer rebalanced(%v)", n)
		case msg, ok := <-j.consumer.Messages():

			if !ok {
				return
			}

			// 更新消息确认偏移
			j.consumer.MarkOffset(msg, "")

			// 消息反序列化(PB)
			pushMsg := new(pb.PushMsg)
			if err := proto.Unmarshal(msg.Value, pushMsg); err != nil {
				log.Errorf("proto.Unmarshal(%v) error(%v)", msg, err)
				continue
			}

			// 消息推送
			if err := j.push(context.Background(), pushMsg); err != nil {
				log.Errorf("j.push(%v) error(%v)", pushMsg, err)
			}
			log.Infof("consume: %s/%d/%d\t%s\t%+v", msg.Topic, msg.Partition, msg.Offset, msg.Key, pushMsg)

		}
	}
}


// 服务发现，更新 comet 列表。
func (j *Job) watchComet(c *naming.Config) {

	dis := naming.New(c)
	resolver := dis.Build("goim.comet")
	event := resolver.Watch()

	select {
	case _, ok := <-event:
		if !ok {
			panic("watchComet init failed")
		}
		if ins, ok := resolver.Fetch(); ok {
			if err := j.newAddress(ins.Instances); err != nil {
				panic(err)
			}
			log.Infof("watchComet init newAddress:%+v", ins)
		}
	case <-time.After(10 * time.Second):
		log.Error("watchComet init instances timeout")
	}

	go func() {
		for {
			if _, ok := <-event; !ok {
				log.Info("watchComet exit")
				return
			}
			ins, ok := resolver.Fetch()
			if ok {
				if err := j.newAddress(ins.Instances); err != nil {
					log.Errorf("watchComet newAddress(%+v) error(%+v)", ins, err)
					continue
				}
				log.Infof("watchComet change newAddress:%+v", ins)
			}
		}
	}()
}

func (j *Job) newAddress(insMap map[string][]*naming.Instance) error {
	ins := insMap[j.c.Env.Zone]
	if len(ins) == 0 {
		return fmt.Errorf("watchComet instance is empty")
	}
	comets := map[string]*Comet{}
	for _, in := range ins {

		// 如果 comet server 有更新，则 job
		if old, ok := j.cometServers[in.Hostname]; ok {
			comets[in.Hostname] = old
			continue
		}
		c, err := NewComet(in, j.c.Comet)
		if err != nil {
			log.Errorf("watchComet NewComet(%+v) error(%v)", in, err)
			return err
		}
		comets[in.Hostname] = c
		log.Infof("watchComet AddComet grpc:%+v", in)
	}
	for key, old := range j.cometServers {
		if _, ok := comets[key]; !ok {
			old.cancel()
			log.Infof("watchComet DelComet:%s", key)
		}
	}
	j.cometServers = comets
	return nil
}
