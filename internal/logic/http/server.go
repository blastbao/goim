package http

import (
	"github.com/blastbao/goim/internal/logic"
	"github.com/blastbao/goim/internal/logic/conf"

	"github.com/gin-gonic/gin"
)

// Server is http server.
type Server struct {
	engine *gin.Engine
	logic  *logic.Logic
}

// New new a http server.
func New(c *conf.HTTPServer, l *logic.Logic) *Server {
	engine := gin.New()
	engine.Use(loggerHandler, recoverHandler)
	go func() {
		if err := engine.Run(c.Addr); err != nil {
			panic(err)
		}
	}()
	s := &Server{
		engine: engine,
		logic:  l,
	}
	s.initRouter()
	return s
}

func (s *Server) initRouter() {
	group := s.engine.Group("/goim")

	// 消息推送接口
	group.POST("/push/keys", s.pushKeys)
	group.POST("/push/mids", s.pushMids)
	group.POST("/push/room", s.pushRoom)
	group.POST("/push/all", s.pushAll)

	// 状态查询接口
	group.GET("/online/top", s.onlineTop)
	group.GET("/online/room", s.onlineRoom)
	group.GET("/online/total", s.onlineTotal)

	// 服务发现
	group.GET("/nodes/weighted", s.nodesWeighted)
	group.GET("/nodes/instances", s.nodesInstances)
}

// Close close the server.
func (s *Server) Close() {

}
