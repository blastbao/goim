package dao

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/blastbao/goim/internal/logic/model"
	log "github.com/golang/glog"
	"github.com/gomodule/redigo/redis"

	"github.com/zhenjl/cityhash"
)

const (
	_prefixMidServer    = "mid_%d" // mid -> key:server
	_prefixKeyServer    = "key_%s" // key -> server
	_prefixServerOnline = "ol_%s"  // server -> online
)

func keyMidServer(mid int64) string {
	return fmt.Sprintf(_prefixMidServer, mid)
}

func keyKeyServer(key string) string {
	return fmt.Sprintf(_prefixKeyServer, key)
}

func keyServerOnline(key string) string {
	return fmt.Sprintf(_prefixServerOnline, key)
}

// pingRedis check redis connection.
// 检查 redis 连接是否正常。
func (d *Dao) pingRedis(c context.Context) (err error) {
	conn := d.redis.Get()
	_, err = conn.Do("SET", "PING", "PONG")
	conn.Close()
	return
}

// 存储 user 信息，主要记录 user 的 uid/key 和 server 的映射关系。
//
// HSET mid_`uid` `user_key` `server`
// SET key_`user_key` `server`
//
func (d *Dao) AddMapping(c context.Context, mid int64, key, server string) (err error) {

	conn := d.redis.Get()
	defer conn.Close()

	var n = 2

	if mid > 0 {

		// HSET mid_`uid` `user_key` `server`
		if err = conn.Send("HSET", keyMidServer(mid), key, server); err != nil {
			log.Errorf("conn.Send(HSET %d,%s,%s) error(%v)", mid, server, key, err)
			return
		}

		// 设置 key 过期时间，对于 hash 表，超时时间只能设置在大 key 上。
		if err = conn.Send("EXPIRE", keyMidServer(mid), d.redisExpire); err != nil {
			log.Errorf("conn.Send(EXPIRE %d,%s,%s) error(%v)", mid, key, server, err)
			return
		}

		n += 2
	}

	// SET key_`user_key` `server`
	if err = conn.Send("SET", keyKeyServer(key), server); err != nil {
		log.Errorf("conn.Send(HSET %d,%s,%s) error(%v)", mid, server, key, err)
		return
	}

	// EXPIRE key_`user_key` expiration
	if err = conn.Send("EXPIRE", keyKeyServer(key), d.redisExpire); err != nil {
		log.Errorf("conn.Send(EXPIRE %d,%s,%s) error(%v)", mid, key, server, err)
		return
	}

	// Flush
	if err = conn.Flush(); err != nil {
		log.Errorf("conn.Flush() error(%v)", err)
		return
	}


	// Pipeline
	for i := 0; i < n; i++ {
		if _, err = conn.Receive(); err != nil {
			log.Errorf("conn.Receive() error(%v)", err)
			return
		}
	}

	return
}

// ExpireMapping expire a mapping.
func (d *Dao) ExpireMapping(c context.Context, mid int64, key string) (has bool, err error) {
	conn := d.redis.Get()
	defer conn.Close()
	var n = 1
	if mid > 0 {
		if err = conn.Send("EXPIRE", keyMidServer(mid), d.redisExpire); err != nil {
			log.Errorf("conn.Send(EXPIRE %d,%s) error(%v)", mid, key, err)
			return
		}
		n++
	}
	if err = conn.Send("EXPIRE", keyKeyServer(key), d.redisExpire); err != nil {
		log.Errorf("conn.Send(EXPIRE %d,%s) error(%v)", mid, key, err)
		return
	}
	if err = conn.Flush(); err != nil {
		log.Errorf("conn.Flush() error(%v)", err)
		return
	}
	for i := 0; i < n; i++ {
		if has, err = redis.Bool(conn.Receive()); err != nil {
			log.Errorf("conn.Receive() error(%v)", err)
			return
		}
	}
	return
}

// DelMapping del a mapping.
func (d *Dao) DelMapping(c context.Context, mid int64, key, server string) (has bool, err error) {
	conn := d.redis.Get()
	defer conn.Close()
	n := 1
	if mid > 0 {
		if err = conn.Send("HDEL", keyMidServer(mid), key); err != nil {
			log.Errorf("conn.Send(HDEL %d,%s,%s) error(%v)", mid, key, server, err)
			return
		}
		n++
	}
	if err = conn.Send("DEL", keyKeyServer(key)); err != nil {
		log.Errorf("conn.Send(HDEL %d,%s,%s) error(%v)", mid, key, server, err)
		return
	}
	if err = conn.Flush(); err != nil {
		log.Errorf("conn.Flush() error(%v)", err)
		return
	}
	for i := 0; i < n; i++ {
		if has, err = redis.Bool(conn.Receive()); err != nil {
			log.Errorf("conn.Receive() error(%v)", err)
			return
		}
	}
	return
}


// ServersByKeys get a server by key.
//
// 根据 use keys 查寻 Redis ，用 Mget 批量获取对应的 server names 。
func (d *Dao) ServersByKeys(c context.Context, keys []string) (res []string, err error) {
	conn := d.redis.Get()
	defer conn.Close()
	var args []interface{}
	for _, key := range keys {
		args = append(args, keyKeyServer(key))
	}
	if res, err = redis.Strings(conn.Do("MGET", args...)); err != nil {
		log.Errorf("conn.Do(MGET %v) error(%v)", args, err)
	}
	return
}

// KeysByMids get a key server by mid.
//
// 根据 uid 查寻 Redis 中哈希结构(HGETALL) ，获取其对应的所有 subkey(user_key) 和 value(server names)，把这些
// user_key => server name 的映射汇总后保存到 ress 中，作为结果返回。

func (d *Dao) KeysByMids(c context.Context, mids []int64) (ress map[string]string, olMids []int64, err error) {

	conn := d.redis.Get()
	defer conn.Close()

	ress = make(map[string]string)

	// 对每个 mid 都发送一个 HGETALL 的 Redis 请求获取 subkey 和 value 。
	for _, mid := range mids {
		if err = conn.Send("HGETALL", keyMidServer(mid)); err != nil {
			log.Errorf("conn.Do(HGETALL %d) error(%v)", mid, err)
			return
		}
	}
	if err = conn.Flush(); err != nil {
		log.Errorf("conn.Flush() error(%v)", err)
		return
	}

	// 对每个 mid 都等待 HGETALL 查寻结果的返回
	for idx := 0; idx < len(mids); idx++ {
		var (
			res map[string]string
		)
		if res, err = redis.StringMap(conn.Receive()); err != nil {
			log.Errorf("conn.Receive() error(%v)", err)
			return
		}

		// 如果 mid 有结果返回，意味着该 mid 是在线的，把当前 mid 保存到 olMids 中。
		if len(res) > 0 {
			olMids = append(olMids, mids[idx])
		}

		// 把 subkey -> value 的映射关系保存到 ress 中
		for k, v := range res {
			ress[k] = v
		}
	}

	//
	return
}

// AddServerOnline add a server online.
//
// 更新 server 下的所有房间的在线用户数目。
func (d *Dao) AddServerOnline(c context.Context, server string, online *model.Online) (err error) {

	// roomsMap 对应的 Redis  HSET 存储结构:
	// 	key = cityhash.CityHash32([]byte(roomID), uint32(len(roomID)))%64
	// 	subKey = roomID
	// 	value = online user count
	//
	// 可见在 roomsMap 存储结构中，对 roomID 进行了分组，根据 roomID 的哈希值分成了 64 组，目的可能是减少 redis key 的数目 ？？？
	roomsMap := map[uint32]map[string]int32{}


	// online.RoomCount = map[roomID][online user count]
	for room, count := range online.RoomCount {

		// 计算并取出 roomID 归属的分组 rMap
		rMap := roomsMap[cityhash.CityHash32([]byte(room), uint32(len(room)))%64]

		// 若 rMap 为空就新建一个非空分组塞到 roomsMap 中
		if rMap == nil {
			rMap = make(map[string]int32)
			roomsMap[cityhash.CityHash32([]byte(room), uint32(len(room)))%64] = rMap
		}

		// 把 roomID => count 的映射保存到当前分组里
		rMap[room] = count
	}

	// key = ol_`server`
	key := keyServerOnline(server)

	// 遍历 roomsMap 中的所有分组，以分组 hashKey 作为 subkey 塞到 key = ol_`server` 的 HSET 中。
	for hashKey, value := range roomsMap {

		//
		err = d.addServerOnline(c, key, strconv.FormatInt(int64(hashKey), 10), &model.Online{RoomCount: value, Server: online.Server, Updated: online.Updated})
		if err != nil {
			return
		}


	}
	return
}

// 以 room 分组的维度，以分组 hashKey 作为 subkey 塞到 key = ol_`server` 的 HSET 中。
//
// HSET key room_group_hashKey jsonBody

func (d *Dao) addServerOnline(c context.Context, key string, hashKey string, online *model.Online) (err error) {
	conn := d.redis.Get()
	defer conn.Close()

	// 用 json.Marshal() 得到 value
	b, _ := json.Marshal(online)

	// key = ol_`server`
	// hashKey = cityhash.CityHash32([]byte(roomID), uint32(len(roomID)))%64
	if err = conn.Send("HSET", key, hashKey, b); err != nil {
		log.Errorf("conn.Send(SET %s,%s) error(%v)", key, hashKey, err)
		return
	}

	if err = conn.Send("EXPIRE", key, d.redisExpire); err != nil {
		log.Errorf("conn.Send(EXPIRE %s) error(%v)", key, err)
		return
	}

	if err = conn.Flush(); err != nil {
		log.Errorf("conn.Flush() error(%v)", err)
		return
	}

	for i := 0; i < 2; i++ {
		if _, err = conn.Receive(); err != nil {
			log.Errorf("conn.Receive() error(%v)", err)
			return
		}
	}
	return
}

// ServerOnline get a server online.

// 根据 server name 取各房间总在线人数。

func (d *Dao) ServerOnline(c context.Context, server string) (online *model.Online, err error) {

	online = &model.Online{RoomCount: map[string]int32{}}

	// 根据 server name 获取该 server 上各个房间的在线人数的存储 key，key => hashkey => JsonValue 。
	key := keyServerOnline(server)

	// 因为在存储时将 server 上的房间划分到 64 个分组中，每个分组对应 HSET 的一个 subKey，这里逐一取出个分组，然后进行数据汇总。
	for i := 0; i < 64; i++ {

		// 根据 key 和 hashKey 取出房间分组信息
		ol, err := d.serverOnline(c, key, strconv.FormatInt(int64(i), 10))

		if err == nil && ol != nil {
			online.Server = ol.Server
			if ol.Updated > online.Updated {
				online.Updated = ol.Updated
			}

			// 遍历分组内的 `房间` 及其 `在线人数` ，并汇总到结果集合 online.RoomCount[] 中。
			for room, count := range ol.RoomCount {
				online.RoomCount[room] = count
			}
		}
	}

	return
}


// 根据 key 和 hashKey 取出房间分组信息
func (d *Dao) serverOnline(c context.Context, key string, hashKey string) (online *model.Online, err error) {
	conn := d.redis.Get()
	defer conn.Close()
	b, err := redis.Bytes(conn.Do("HGET", key, hashKey))
	if err != nil {
		if err != redis.ErrNil {
			log.Errorf("conn.Do(HGET %s %s) error(%v)", key, hashKey, err)
		}
		return
	}
	online = new(model.Online)
	if err = json.Unmarshal(b, online); err != nil {
		log.Errorf("serverOnline json.Unmarshal(%s) error(%v)", b, err)
		return
	}
	return
}

// DelServerOnline del a server online.
func (d *Dao) DelServerOnline(c context.Context, server string) (err error) {
	conn := d.redis.Get()
	defer conn.Close()
	key := keyServerOnline(server)
	if _, err = conn.Do("DEL", key); err != nil {
		log.Errorf("conn.Do(DEL %s) error(%v)", key, err)
	}
	return
}
