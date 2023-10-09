package web

import (
	"github.com/valyala/fasthttp"
	"io"
	"yerfYar/server"
)

const defaultBufSize = 512 * 1024

type Server struct {
	s *server.InMemory
}

func NewServer(s *server.InMemory) *Server {
	return &Server{s: s}
}

func (s *Server) handler(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Path()) {
	case "/write":
		s.writeHandler(ctx)
	case "/read":
		s.readHandler(ctx)
	default:
		ctx.WriteString("hello yerfYar!")
	}
}

func (s *Server) writeHandler(ctx *fasthttp.RequestCtx) {
	ctx.WriteString("you chose to write...")
	if err := s.s.Send(ctx.Request.Body()); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
	}
}

func (s *Server) readHandler(ctx *fasthttp.RequestCtx) {
	ctx.WriteString("you chose to read...")
	buf := make([]byte, defaultBufSize)

	res, err := s.s.Receive(buf)
	if err != nil && err != io.EOF {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError) // 设置HTTP响应状态码为500表示server内部发生错误
		ctx.WriteString(err.Error())
		return
	}
	ctx.Write(res)
}

func (s *Server) Serve() error {
	return fasthttp.ListenAndServe(":8080", s.handler)
}
