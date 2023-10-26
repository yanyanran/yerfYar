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
)

const defaultScratchSize = 64 * 1024

var errRetry = errors.New("please retry the request")

type ReadOffset struct {
	CurChunk          protocol.Chunk
	LastAckedChunkIdx int
	Offset            uint64
}

type state struct {
	Offsets map[string]*ReadOffset
}

type Simple struct {
	Debug  bool
	Logger *log.Logger

	addrs []string
	cl    *Raw
	st    *state
}

func NewSimple(addrs []string) *Simple {
	return &Simple{
		addrs: addrs,
		cl:    NewRaw(&http.Client{}),
		st:    &state{Offsets: make(map[string]*ReadOffset)},
	}
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

func (s *Simple) Send(ctx context.Context, category string, msgs []byte) error {
	return s.cl.Write(ctx, s.getAddr(), category, msgs)
}

// Process 等待新消息或出现问题时返回错误。
// scratch缓冲区用于读取数据。
// 仅当 processFn() 对于正在处理的数据没有返回错误时，读取偏移量才会提前。
func (s *Simple) Process(ctx context.Context, category string, scratch []byte, processFn func([]byte) error) error {
	if scratch == nil {
		scratch = make([]byte, defaultScratchSize)
	}
	addr := s.getAddr()

	if len(s.st.Offsets) == 0 {
		if err := s.updateCurrentChunks(ctx, category, addr); err != nil {
			return fmt.Errorf("[Process]updateCurrentChunk: %w", err)
		}
	}

	for instance := range s.st.Offsets {
		err := s.processInstance(ctx, addr, instance, category, scratch, processFn)
		if errors.Is(err, io.EOF) {
			continue
		}
		return err
	}
	return io.EOF
}

func (s *Simple) processInstance(ctx context.Context, addr, instance, category string, scratch []byte, processFn func([]byte) error) error {
	for {
		if err := s.updateCurrentChunks(ctx, category, addr); err != nil {
			return fmt.Errorf("[processInstance]updateCurrentChunk: %w", err)
		}

		err := s.process(ctx, addr, instance, category, scratch, processFn)
		if err == errRetry {
			if s.Debug {
				s.logger().Printf("正在重试读取类别 %q ...(获取到错误：%v)", category, err)
			}
			continue
		}
		return nil
	}
}

func (s *Simple) process(ctx context.Context, addr, instance, category string, scratch []byte, processFn func([]byte) error) error {
	curCh := s.st.Offsets[instance]

	res, found, err := s.cl.Read(ctx, addr, category, curCh.CurChunk.Name, curCh.Offset, scratch)
	if err != nil {
		return err
	} else if !found {
		if s.Debug {
			s.logger().Printf("chunk %+v 在 %q 处丢失，可能没复制，跳过", curCh.CurChunk.Name, addr)
		}
		return nil
	}
	// 读0个字节但没错，意味着按照约定文件结束
	if len(res) == 0 {
		if !curCh.CurChunk.Complete {
			if err := s.updateCurrentChunkCompleteStatus(ctx, curCh, instance, category, addr); err != nil {
				return fmt.Errorf("updateCurrentChunkCompleteStatus: %v", err)
			}
			if !curCh.CurChunk.Complete {
				// 读到了最后且在请求间无新数据
				if curCh.Offset >= curCh.CurChunk.Size {
					return io.EOF
				}
			} else {
				// 在我们发送读取请求和chunk Complete间出现了新数据
				return errRetry
			}
		}

		// 该chunk已被标记完成，但在发送读取请求和chunk Complete之间出现了新数据
		if curCh.Offset < curCh.CurChunk.Size {
			if s.Debug {
				s.logger().Printf(`errRetry：chunk %q 已标记为完成。然而在发送读取请求和chunk完成之间出现了新数据。 (curCh.Off < curCh.CurChunk.Size) = (%v < %v)`,
					curCh.CurChunk.Name, curCh.Offset, curCh.CurChunk.Size)
			}
			return errRetry
		}

		// 每读取完一个chunk就会ack一次
		if err := s.cl.Ack(ctx, addr, category, curCh.CurChunk.Name, curCh.Offset); err != nil {
			return fmt.Errorf("ack current chunk: %v", err)
		}
		// 需要读取下一个chunk，这样我们就不会返回空响应
		_, idx := protocol.ParseChunkFileName(curCh.CurChunk.Name)
		curCh.LastAckedChunkIdx = idx
		curCh.CurChunk = protocol.Chunk{}
		curCh.Offset = 0

		s.st.Offsets[instance] = curCh

		if s.Debug {
			s.logger().Printf(`errRetry: 需要读取下一个chunk，这样就不会返回空响应`)
		}
		return errRetry
	}

	err = processFn(res)
	if err == nil {
		curCh.Offset += uint64(len(res))
		s.st.Offsets[instance] = curCh
	}

	return err
}

func (s *Simple) updateCurrentChunks(ctx context.Context, category, addr string) error {
	chunks, err := s.cl.ListChunks(ctx, addr, category, false)
	if err != nil {
		return fmt.Errorf("listChunks failed: %v", err)
	}

	if len(chunks) == 0 {
		return io.EOF
	}

	chunksByInstance := make(map[string][]protocol.Chunk)
	for _, c := range chunks {
		instance, chunkIdx := protocol.ParseChunkFileName(c.Name)
		if chunkIdx < 0 {
			continue
		}

		// 我们可以拥有已经在其他副本中确认的chunk（因为复制是异步的），所以需要跳过它们
		curChunk, exists := s.st.Offsets[instance]
		if exists && chunkIdx <= curChunk.LastAckedChunkIdx {
			continue
		}

		chunksByInstance[instance] = append(chunksByInstance[instance], c)
	}

	for instance, chunks := range chunksByInstance {
		curChunk, exists := s.st.Offsets[instance]
		if !exists {
			curChunk = &ReadOffset{}
		}

		// 在两种情况下名称为空：
		// 1、第一次尝试阅读这个例子
		// 2、读取最新的块直到最后，并且需要开始读取新的块
		if curChunk.CurChunk.Name == "" {
			curChunk.CurChunk = s.getOldestChunk(chunks)
			curChunk.Offset = 0
		}

		s.st.Offsets[instance] = curChunk
	}

	return nil
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

func (s *Simple) updateCurrentChunkCompleteStatus(ctx context.Context, curCh *ReadOffset, instance, category, addr string) error {
	chunks, err := s.cl.ListChunks(ctx, addr, category, false)
	if err != nil {
		return fmt.Errorf("listChunks failed: %v", err)
	}

	for _, c := range chunks {
		chunkInstance, idx := protocol.ParseChunkFileName(c.Name)
		if idx < 0 {
			continue
		}
		if chunkInstance != instance {
			continue
		}
		if c.Name == curCh.CurChunk.Name {
			curCh.CurChunk = c
			return nil
		}
	}
	return nil
}
