package logic

import (
	"context"
	"sort"
	"strings"

	"github.com/blastbao/goim/internal/logic/model"
)

var (
	_emptyTops = make([]*model.Top, 0)
)

// OnlineTop get the top online.
// 取出在线人数最高的 n 个房间
func (l *Logic) OnlineTop(c context.Context, typ string, n int) (tops []*model.Top, err error) {


	// key = roomType://roomID
	// cnt = onlineUserCnt，房间在线人数
	for key, cnt := range l.roomCount {

		// 由于 roomID 的构造规则是 type://id ，所以要用前缀来检查 typ 是否匹配
		if strings.HasPrefix(key, typ) {

			// 从 key 解析出 roomID
			_, roomID, err := model.DecodeRoomKey(key)
			if err != nil {
				continue
			}

			// 构造 Top 目的是用 cnt 进行排序
			top := &model.Top{
				RoomID: roomID,
				Count:  cnt,
			}
			tops = append(tops, top)
		}
	}

	// 按房间在线人数 cnt 对 room 进行降序排序
	sort.Slice(tops, func(i, j int) bool {
		return tops[i].Count > tops[j].Count
	})

	// 取前 n 个 room
	if len(tops) > n {
		tops = tops[:n]
	}

	if len(tops) == 0 {
		tops = _emptyTops
	}

	return
}

// OnlineRoom get rooms online.

// 根据房间 type 与 room id 取出房间在线人数。
func (l *Logic) OnlineRoom(c context.Context, typ string, rooms []string) (res map[string]int32, err error) {
	res = make(map[string]int32, len(rooms))
	for _, room := range rooms {
		res[room] = l.roomCount[model.EncodeRoomKey(typ, room)]
	}
	return
}

// OnlineTotal get all online.
// 取出当前
func (l *Logic) OnlineTotal(c context.Context) (int64, int64) {
	return l.totalIPs, l.totalConns
}
