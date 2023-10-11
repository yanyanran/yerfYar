package server

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
)

const maxInMemoryChunkSize = 10 * 1024 * 1024 // bytes

var (
	errBufTooSmall = errors.New("buffer is too small to fit a single message")
)

type InMemory struct {
	sync.RWMutex
	lastChunk     string
	lastChunkSize uint64
	lastChunkIdx  uint64
	bufs          map[string][]byte // 缓冲区
}

func (s *InMemory) Write(msgs []byte) error {
	s.Lock()
	defer s.Unlock()

	// new a chunk
	if s.lastChunk == "" || (s.lastChunkSize+uint64(len(msgs)) > maxInMemoryChunkSize) {
		s.lastChunk = fmt.Sprintf("chunk%d", s.lastChunkIdx)
		s.lastChunkSize = 0
		s.lastChunkIdx++
	}

	if s.bufs == nil {
		s.bufs = make(map[string][]byte) // 初始化缓冲区
	}

	s.bufs[s.lastChunk] = append(s.bufs[s.lastChunk], msgs...)
	s.lastChunkSize += uint64(len(msgs))
	return nil
}

func (s *InMemory) Read(chunk string, off uint64, maxSize uint64, w io.Writer) error {
	s.RLock()
	defer s.RUnlock()

	buf, ok := s.bufs[chunk]
	if !ok { // chunk不存在
		return fmt.Errorf("chunk %q 不存在！", chunk)
	}

	maxOff := uint64(len(buf))
	if off >= maxOff {
		return nil
	} else if off+maxSize >= maxOff { // 只需读取剩余数据
		// 将其写入到目标io.Writer中
		w.Write(buf[off:])
		return nil
	}

	truncated, _, err := cutToLastMessage(buf[off : off+maxSize])
	if err != nil {
		return err
	}

	if _, err := w.Write(truncated); err != nil {
		return err
	}

	return nil
}

func (s *InMemory) Ack(chunk string) error {
	s.Lock()
	defer s.Unlock()

	_, ok := s.bufs[chunk]
	if !ok {
		return fmt.Errorf("chunk %q 不存在！", chunk)
	}

	if chunk == s.lastChunk {
		return fmt.Errorf("chunk %q 当前正在写入，无法ack", chunk)
	}

	delete(s.bufs, chunk)
	return nil
}

// ListChunks 返回系统中存在的chunk列表
func (s *InMemory) ListChunks() ([]Chunk, error) {
	s.RLock()
	defer s.RUnlock()

	res := make([]Chunk, 0, len(s.bufs))
	for chunk := range s.bufs {
		var c Chunk
		c.Complete = s.lastChunk != chunk // 只有lastChunk正在写入
		c.Name = chunk

		res = append(res, c)
	}

	return res, nil
}

func cutToLastMessage(res []byte) (truncated []byte, rest []byte, err error) {
	n := len(res)

	if n == 0 {
		return res, nil, nil
	}

	if res[n-1] == '\n' {
		return res, nil, nil
	}

	lastPos := bytes.LastIndexByte(res, '\n')
	if lastPos < 0 {
		return nil, nil, errBufTooSmall
	}

	return res[0 : lastPos+1], res[lastPos+1:], nil
}
