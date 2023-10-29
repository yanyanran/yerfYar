package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/yanyanran/yerfYar/protocol"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"time"
)

const defaultScratchSize = 64 * 1024

var errRetry = errors.New("please retry the request")
var errNoNewChunks = errors.New("没有新的chunk")

type ReadOffset struct {
	CurChunk          protocol.Chunk
	LastAckedChunkIdx int
	Offset            uint64
}

type state struct {
	Offsets map[string]*ReadOffset
}

type Simple struct {
	debug  bool
	Logger *log.Logger

	ack          bool
	pollInterval time.Duration
	addrs        []string
	cl           *Raw
	st           *state
}

func NewSimple(addrs []string) *Simple {
	return &Simple{
		ack:          true,
		pollInterval: time.Millisecond * 100,
		addrs:        addrs,
		cl:           NewRaw(&http.Client{}),
		st:           &state{Offsets: make(map[string]*ReadOffset)},
	}
}

// SetPollInterval 设置没有新msg时用于轮询yerkYar的事件间隔（默认值100ms）
func (s *Simple) SetPollInterval(d time.Duration) {
	s.pollInterval = d
}

func (s *Simple) SetDebug(v bool) {
	s.debug = v
	s.cl.SetDebug(v)
}

func (s *Simple) SetMinSyncReplicas(v uint) {
	s.cl.SetMinSyncReplicas(v)
}

func (s *Simple) SetAck(v bool) {
	s.ack = v
}

// MarshalState go-> json
func (s *Simple) MarshalState() ([]byte, error) {
	return json.Marshal(s.st)
}

// RestoreSavedState json-> go
func (s *Simple) RestoreSavedState(buf []byte) error {
	return json.Unmarshal(buf, &s.st)
}

func (s *Simple) getAddr() string {
	addrIdx := rand.Intn(len(s.addrs))
	return s.addrs[addrIdx]
}

func (s *Simple) logger() *log.Logger {
	if s.Logger == nil {
		return log.Default()
	}
	return s.Logger
}

// Send TODO 不要只写一个随机地址，写到首选server，在请求失败时尝试下一个
func (s *Simple) Send(ctx context.Context, category string, msgs []byte) error {
	return s.cl.Write(ctx, s.getAddr(), category, msgs)
}

// Process 等待新消息或出现问题时返回错误。
// scratch缓冲区用于读取数据，缓冲区大小决定了将要读取的最大批处理大小
// 仅当 processFn() 对于正在处理的数据没有返回错误时，读取偏移量才会提前。
func (s *Simple) Process(ctx context.Context, category string, scratch []byte, processFn func([]byte) error) error {
	if scratch == nil {
		scratch = make([]byte, defaultScratchSize)
	}

	for {
		err := s.tryProcess(ctx, category, scratch, processFn)
		if err == nil {
			return nil
		} else if errors.Is(err, errRetry) {
			continue
		} else if !errors.Is(err, io.EOF) {
			return err
		}

		select {
		case <-time.After(s.pollInterval):
		case <-ctx.Done():
			return context.Canceled
		}
	}
}

func (s *Simple) tryProcess(ctx context.Context, category string, scratch []byte, processFn func([]byte) error) error {
	addr := s.getAddr()

	if err := s.updateOffsets(ctx, category, addr); err != nil {
		return fmt.Errorf("updateCurrentChunk: %w", err)
	}

	needRetry := false

	for instance := range s.st.Offsets {
		err := s.processInstance(ctx, addr, instance, category, scratch, processFn)
		if errors.Is(err, io.EOF) {
			continue
		} else if errors.Is(err, errRetry) {
			needRetry = true
			continue
		}
		return err
	}
	if needRetry {
		return errRetry
	}
	return io.EOF
}

func (s *Simple) updateOffsets(ctx context.Context, category, addr string) error {
	lastAckedChunkIndexes := make(map[string]int, len(s.st.Offsets))
	for instance, off := range s.st.Offsets {
		lastAckedChunkIndexes[instance] = off.LastAckedChunkIdx
	}

	chunksByInstance, err := s.getUnackedChunksGroupedByInstance(ctx, category, addr, lastAckedChunkIndexes)
	if err != nil {
		return err
	}

	for instance, chunks := range chunksByInstance {
		off, exists := s.st.Offsets[instance]
		if exists {
			s.updateCurrentChunkInfo(chunks, off)
			continue
		}

		s.st.Offsets[instance] = &ReadOffset{
			CurChunk:          s.getOldestChunk(chunks),
			LastAckedChunkIdx: -1, // 不能用0，因为这意味着chunk 0 被ack
			Offset:            0,
		}
	}
	return nil
}

func (s *Simple) processInstance(ctx context.Context, addr, instance, category string, scratch []byte, processFn func([]byte) error) error {
	curCh := s.st.Offsets[instance]
	res, found, err := s.cl.Read(ctx, addr, category, curCh.CurChunk.Name, curCh.Offset, scratch)
	if err != nil {
		return err
	} else if !found {
		// TODO 如果存在比正在处理的chunk更新的chunk-> 更新当前chunk
		// TODO 如果现在正在读取的chunk已被ack，那么将永远不会读取新的chunk。状态过时（客户端崩溃且没有保存最后一个offset）就会发生这种情况
		if s.debug {
			s.logger().Printf("chunk %+v 在 %q 处丢失，可能没复制，跳过", curCh.CurChunk.Name, addr)
		}
		return io.EOF
	}

	if len(res) > 0 {
		err = processFn(res)
		if err == nil {
			curCh.Offset += uint64(len(res))
		}
		return err
	}

	// 读0个字节但没错，意味着按照约定文件结束
	if !curCh.CurChunk.Complete {
		if s.debug {
			s.logger().Printf("chunk %+v 一直读取到最后并且未完成，EOF", curCh.CurChunk.Name)
		}
		return io.EOF
	}

	// 在ack当前chunk之前获取下一个chunk，这样就总是有非空的chunk名称。如果我们没有新chunk名称，可能chunk列表是从另一个还没有新chunk的副本中获取到的
	nextChunk, err := s.getNextChunkForInstance(ctx, addr, instance, category, curCh.CurChunk.Name)
	if errors.Is(err, errNoNewChunks) {
		if s.debug {
			s.logger().Printf("chunk %+v 之后没有新chunk，EOF", curCh.CurChunk.Name)
		}
		return io.EOF
	} else if err != nil {
		return err
	}

	if s.ack {
		if err := s.cl.Ack(ctx, addr, category, curCh.CurChunk.Name, curCh.Offset); err != nil {
			return fmt.Errorf("ack当前chunk发生错误: %w", err)
		}
	}

	if s.debug {
		s.logger().Printf("将下一个chunk设置为 %q", nextChunk.Name)
	}

	_, idx := protocol.ParseChunkFileName(curCh.CurChunk.Name)
	curCh.LastAckedChunkIdx = idx
	curCh.CurChunk = nextChunk
	curCh.Offset = 0

	if s.debug {
		s.logger().Printf(`errRetry: 需要读取下一个chunk，避免返回空响应`)
	}
	return errRetry
}

func (s *Simple) getNextChunkForInstance(ctx context.Context, addr, instance, category string, chunkName string) (protocol.Chunk, error) {
	_, idx := protocol.ParseChunkFileName(chunkName)
	lastAckedChunkIndexes := make(map[string]int)
	lastAckedChunkIndexes[instance] = idx
	chunksByInstance, err := s.getUnackedChunksGroupedByInstance(ctx, category, addr, lastAckedChunkIndexes)
	if err != nil {
		return protocol.Chunk{}, fmt.Errorf("getting chunks list before ack: %w", err)
	}
	if len(chunksByInstance[instance]) == 0 {
		return protocol.Chunk{}, fmt.Errorf("getting new chunks list before ack: unexpected error for instance %q: %w", instance, errNoNewChunks)
	}

	return chunksByInstance[instance][0], nil
}

func (s *Simple) getUnackedChunksGroupedByInstance(ctx context.Context, category, addr string, lastAckedChunkIndexes map[string]int) (map[string][]protocol.Chunk, error) {
	chunks, err := s.cl.ListChunks(ctx, addr, category, false)
	if err != nil {
		return nil, fmt.Errorf("listChunks failed: %w", err)
	}

	if len(chunks) == 0 {
		return nil, io.EOF
	}

	chunksByInstance := make(map[string][]protocol.Chunk)
	for _, c := range chunks {
		instance, chunkIdx := protocol.ParseChunkFileName(c.Name)
		if chunkIdx < 0 {
			continue
		}

		// 我们可以拥有已经在其他副本中确认的chunk（因为复制是异步的），所以需要跳过它们
		lastAckedChunkIdx, exists := lastAckedChunkIndexes[instance]
		if exists && chunkIdx <= lastAckedChunkIdx {
			continue
		}
		chunksByInstance[instance] = append(chunksByInstance[instance], c)
	}
	return chunksByInstance, nil
}

func (s *Simple) getOldestChunk(chunks []protocol.Chunk) protocol.Chunk {
	// 优先考虑完整的chunk
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].Name < chunks[j].Name
	})
	for _, c := range chunks {
		if c.Complete {
			return c
		}
	}
	return chunks[0]
}

func (s *Simple) updateCurrentChunkInfo(chunks []protocol.Chunk, curCh *ReadOffset) {
	for _, c := range chunks {
		_, idx := protocol.ParseChunkFileName(c.Name)
		if idx < 0 {
			continue
		}

		if c.Name == curCh.CurChunk.Name {
			curCh.CurChunk = c
			return
		}
	}
}
