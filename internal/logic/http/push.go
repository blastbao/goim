package http

import (
	"context"
	"io/ioutil"

	"github.com/gin-gonic/gin"
)


// 根据 user key 推送消息到 kafka 。
func (s *Server) pushKeys(c *gin.Context) {
	var arg struct {
		Op   int32    `form:"operation"`
		Keys []string `form:"keys"`
	}
	if err := c.BindQuery(&arg); err != nil {
		errors(c, RequestErr, err.Error())
		return
	}
	// read message
	msg, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		errors(c, RequestErr, err.Error())
		return
	}
	if err = s.logic.PushKeys(context.TODO(), arg.Op, arg.Keys, msg); err != nil {
		result(c, nil, RequestErr)
		return
	}
	result(c, nil, OK)
}


// 根据 user id 推送消息到 kafka 。
func (s *Server) pushMids(c *gin.Context) {

	var arg struct {
		Op   int32   `form:"operation"`
		Mids []int64 `form:"mids"`
	}

	if err := c.BindQuery(&arg); err != nil {
		errors(c, RequestErr, err.Error())
		return
	}

	// read message from body.
	msg, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		errors(c, RequestErr, err.Error())
		return
	}

	// 根据 user id 推送消息。
	if err = s.logic.PushMids(context.TODO(), arg.Op, arg.Mids, msg); err != nil {
		errors(c, ServerErr, err.Error())
		return
	}

	// 回复 ok 。
	result(c, nil, OK)
}

// 根据 room id 推送消息到 kafka 。
func (s *Server) pushRoom(c *gin.Context) {


	var arg struct {
		Op   int32  `form:"operation" binding:"required"`
		Type string `form:"type" binding:"required"`
		Room string `form:"room" binding:"required"`
	}

	if err := c.BindQuery(&arg); err != nil {
		errors(c, RequestErr, err.Error())
		return
	}

	// read message from body.
	msg, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		errors(c, RequestErr, err.Error())
		return
	}

	// 发送消息到 kafka。
	if err = s.logic.PushRoom(c, arg.Op, arg.Type, arg.Room, msg); err != nil {
		errors(c, ServerErr, err.Error())
		return
	}

	// 回复 ok 。
	result(c, nil, OK)
}


// 根据 operation 推送消息到 kafka 。
func (s *Server) pushAll(c *gin.Context) {
	var arg struct {
		Op    int32 `form:"operation" binding:"required"`
		Speed int32 `form:"speed"`
	}
	if err := c.BindQuery(&arg); err != nil {
		errors(c, RequestErr, err.Error())
		return
	}

	// read message from body.
	msg, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		errors(c, RequestErr, err.Error())
		return
	}

	// 发送消息到 kafka。
	if err = s.logic.PushAll(c, arg.Op, arg.Speed, msg); err != nil {
		errors(c, ServerErr, err.Error())
		return
	}

	// 回复 ok 。
	result(c, nil, OK)
}
