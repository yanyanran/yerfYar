package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/valyala/fasthttp"
	"github.com/yanyanran/yerfYar/server"
	"github.com/yanyanran/yerfYar/server/replication"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Server struct {
	logger       *log.Logger
	instanceName string
	dirname      string
	listenAddr   string

	replClient  *replication.State
	replStorage *replication.Storage

	getOnDisk GetOnDiskFn
}

type GetOnDiskFn func(category string) (*server.OnDisk, error)

func NewServer(logger *log.Logger, replClient *replication.State, instanceName string, dirname string, listenAddr string, replStorage *replication.Storage, getOnDisk GetOnDiskFn) *Server {
	return &Server{
		logger:       logger,
		instanceName: instanceName,
		dirname:      dirname,
		listenAddr:   listenAddr,
		replClient:   replClient,
		replStorage:  replStorage,
		getOnDisk:    getOnDisk,
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
	case "/replication/ack":
		s.replicationAckHandler(ctx)
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

	return s.getOnDisk(category)
}

func (s *Server) writeHandler(ctx *fasthttp.RequestCtx) {
	storage, err := s.getStorageForCategory(string(ctx.QueryArgs().Peek("category")))
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(err.Error())
		return
	}

	chunkName, off, err := storage.Write(ctx, ctx.Request.Body())
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
		return
	}
	minSyncReplicas, err := ctx.QueryArgs().GetUint("min_sync_replicas")
	if err != nil && err != fasthttp.ErrNoArgValue {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(err.Error())
		return
	} else if minSyncReplicas > 0 {
		waitCtx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()

		if err := storage.Wait(waitCtx, chunkName, uint64(off), uint(minSyncReplicas)); err != nil {
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			ctx.WriteString(err.Error())
			return
		}
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

	if err := storage.Ack(ctx, string(chunk), uint64(size)); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
	}
}

// replicationAckHandler 用于让chunk所有者（我们）知道复制副本已成功下载chunk
func (s *Server) replicationAckHandler(ctx *fasthttp.RequestCtx) {
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

	// instance是已成功下载相应chunk部分的实例名称
	instance := ctx.QueryArgs().Peek("instance")
	if len(chunk) == 0 {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString("错误的 `instance` GET参数：必须提供副本名称")
		return
	}

	size, err := ctx.QueryArgs().GetUint("size")
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(fmt.Sprintf("bad `fromOff` GET param: %v", err))
		return
	}

	storage.ReplicationAck(ctx, string(chunk), string(instance), uint64(size))
}

func (s *Server) readHandler(ctx *fasthttp.RequestCtx) {
	chunk := ctx.QueryArgs().Peek("chunk") // 获取查询参数kv，peek查看参数第一个值
	if len(chunk) == 0 {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString("错误的 `chunk` GET参数：必须提供chunk名称")
		return
	}

	fromReplication, _ := ctx.QueryArgs().GetUint("from_replication")
	if fromReplication == 1 {
		// c.logger.Printf("为chunk %v 的复制请求休眠 8 秒", string(chunk))
		// time.Sleep(time.Second * 8)
	}

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

	err = storage.Read(string(chunk), uint64(off), uint64(maxSize), ctx)
	if err != nil && err != io.EOF {
		if errors.Is(err, os.ErrNotExist) {
			ctx.SetStatusCode(fasthttp.StatusNotFound)
		} else {
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		}
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

	fromReplication, _ := ctx.QueryArgs().GetUint("from_replication")
	if fromReplication == 1 {
		// c.logger.Printf("休眠 8 秒以响应来自复制的列出chunk的请求")
		// time.Sleep(time.Second * 8)
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
