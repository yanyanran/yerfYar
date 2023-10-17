package web

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/valyala/fasthttp"
	"github.com/yanyanran/yerfYar/server"
	"github.com/yanyanran/yerfYar/server/replication"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Server struct {
	instanceName string
	dirname      string
	listenAddr   string

	replClient  *replication.Client
	replStorage *replication.Storage

	mu       sync.Mutex
	storages map[string]*server.OnDisk
}

func NewServer(replClient *replication.Client, instanceName string, dirname string, listenAddr string, replStorage *replication.Storage) *Server {
	return &Server{
		instanceName: instanceName,
		dirname:      dirname,
		listenAddr:   listenAddr,
		replClient:   replClient,
		replStorage:  replStorage,
		storages:     make(map[string]*server.OnDisk),
	}
}

func (s *Server) handler(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Path()) {
	case "/write":
		s.writeHandler(ctx)
	case "/read":
		s.readHandler(ctx)
	case "/ack":
		s.ackHandler(ctx)
	case "/listChunks":
		s.listChunksHandler(ctx)
	default:
		ctx.WriteString("hello yerfYar!")
	}
}

func isValidCategory(category string) bool {
	if category == "" {
		return false
	}

	cleanPath := filepath.Clean(category)
	if cleanPath != category {
		return false
	}

	if strings.ContainsAny(category, `/\.`) {
		return false
	}
	return true
}

func (s *Server) getStorageForCategory(category string) (*server.OnDisk, error) {
	if !isValidCategory(category) {
		return nil, errors.New("invalid category name")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	storage, ok := s.storages[category]
	if ok {
		return storage, nil
	}

	dir := filepath.Join(s.dirname, category)
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, fmt.Errorf("creating directory for the category: %v", err)
	}

	storage, err := server.NewOnDisk(dir, category, s.instanceName, s.replStorage)
	if err != nil {
		return nil, err
	}

	s.storages[category] = storage
	return storage, nil
}

func (s *Server) writeHandler(ctx *fasthttp.RequestCtx) {
	storage, err := s.getStorageForCategory(string(ctx.QueryArgs().Peek("category")))
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(err.Error())
		return
	}

	if err := storage.Write(ctx, ctx.Request.Body()); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
	}
}

func (s *Server) ackHandler(ctx *fasthttp.RequestCtx) {
	storage, err := s.getStorageForCategory(string(ctx.QueryArgs().Peek("category")))
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(err.Error())
		return
	}

	chunk := ctx.QueryArgs().Peek("chunk")
	if len(chunk) == 0 {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString("错误的 `chunk` GET参数：必须提供chunk名称")
		return
	}

	size, err := ctx.QueryArgs().GetUint("size")
	if err != nil {
		ctx.WriteString(fmt.Sprintf("错误的 `chunk` GET参数：%v", err))
		return
	}

	if err := storage.Ack(string(chunk), uint64(size)); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
	}
}

func (s *Server) readHandler(ctx *fasthttp.RequestCtx) {
	storage, err := s.getStorageForCategory(string(ctx.QueryArgs().Peek("category")))
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(err.Error())
		return
	}

	off, err := ctx.QueryArgs().GetUint("off")
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(fmt.Sprintf("错误的 `off` GET参数: %v", err))
		return
	}

	maxSize, err := ctx.QueryArgs().GetUint("maxSize")
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(fmt.Sprintf("bad `maxSize` GET param: %v", err))
		return
	}

	chunk := ctx.QueryArgs().Peek("chunk")
	if len(chunk) == 0 {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString("错误的 `chunk` GET参数: 必须提供chunk名称")
		return
	}

	err = storage.Read(string(chunk), uint64(off), uint64(maxSize), ctx)
	if err != nil && err != io.EOF {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
		return
	}
}

func (s *Server) listChunksHandler(ctx *fasthttp.RequestCtx) {
	storage, err := s.getStorageForCategory(string(ctx.QueryArgs().Peek("category")))
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(err.Error())
		return
	}

	chunks, err := storage.ListChunks()
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
		return
	}

	// 将chunks以JSON格式编码，写入到HTTP响应体中
	// chunk列表的JSON数据将作为响应返回给客户端
	json.NewEncoder(ctx).Encode(chunks)
}

func (s *Server) Serve() error {
	return fasthttp.ListenAndServe(s.listenAddr, s.handler)
}
