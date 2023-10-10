package web

import (
	"fmt"
	"github.com/valyala/fasthttp"
	"io"
)

const defaultBufSize = 512 * 1024

// Storage 为后端存储定义了一个接口 (磁盘、内存或其他类型的存储)
type Storage interface {
	Write(msgs []byte) error
	Read(off uint64, maxSize uint64, w io.Writer) error
	Ack() error
}

type Server struct {
	s Storage
}

func NewServer(s Storage) *Server {
	return &Server{s: s}
}

func (s *Server) handler(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Path()) {
	case "/write":
		s.writeHandler(ctx)
	case "/read":
		s.readHandler(ctx)
	case "/ack":
		s.ackHandler(ctx)

	default:
		ctx.WriteString("hello yerfYar!")
	}
}

func (s *Server) writeHandler(ctx *fasthttp.RequestCtx) {
	ctx.WriteString("you chose to write...")
	if err := s.s.Write(ctx.Request.Body()); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
	}
}

func (s *Server) ackHandler(ctx *fasthttp.RequestCtx) {
	if err := s.s.Ack(); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
	}
}

func (s *Server) readHandler(ctx *fasthttp.RequestCtx) {
	off, err := ctx.QueryArgs().GetUint("off")
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(fmt.Sprintf("bad `off` GET param: %v", err))
		return
	}

	maxSize, err := ctx.QueryArgs().GetUint("maxSize")
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(fmt.Sprintf("bad `maxSize` GET param: %v", err))
		return
	}

	err = s.s.Read(uint64(off), uint64(maxSize), ctx)
	if err != nil && err != io.EOF {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
		return
	}
}

func (s *Server) Serve() error {
	return fasthttp.ListenAndServe(":8080", s.handler)
}
