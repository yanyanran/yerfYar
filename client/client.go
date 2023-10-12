package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/yanyanran/yerfYar/server"
	"io"
	"math/rand"
	"net/http"
)

const defaultScratchSize = 64 * 1024

type Simple struct {
	addr     []string
	cl       *http.Client
	curChunk server.Chunk
	offset   uint64
}

func NewSimple(addrs []string) *Simple {
	return &Simple{
		addr: addrs,
		cl:   &http.Client{},
	}
}

func (s *Simple) Send(msgs []byte) error {
	res, err := s.cl.Post(s.addr[0]+"/write", "application/octet-stream", bytes.NewReader(msgs))
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, res.Body)
		return fmt.Errorf("http code %d, %s", res.StatusCode, b.String())
	}

	io.Copy(io.Discard, res.Body)
	return nil
}

func (s *Simple) Receive(scratch []byte) ([]byte, error) {
	if scratch == nil {
		scratch = make([]byte, defaultScratchSize)
	}

	addrIndex := rand.Intn(len(s.addr))
	addr := s.addr[addrIndex]
	if err := s.updateCurrentChunk(addr); err != nil {
		return nil, fmt.Errorf("updateCurrentChunk: %w", err)
	}

	readURL := fmt.Sprintf("%s/read?off=%d&maxSize=%d&chunk=%s", addr, s.offset, len(scratch), s.curChunk.Name)

	res, err := s.cl.Get(readURL)
	if err != nil {
		return nil, fmt.Errorf("read %q: %v", readURL, err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, res.Body)
		return nil, fmt.Errorf("http code %d, %s", res.StatusCode, b.String())
	}
	b := bytes.NewBuffer(scratch[0:0])
	_, err = io.Copy(b, res.Body)
	if err != nil {
		return nil, err
	}

	// 读0个字节但没错，意味着按照约定文件结束
	if b.Len() == 0 {
		if !s.curChunk.Complete {
			if err := s.updateCurrentChunkCompleteStatus(addr); err != nil {
				return nil, fmt.Errorf("updateCurrentChunkCompleteStatus: %v", err)
			}
		}
		if !s.curChunk.Complete {
			return nil, io.EOF
		}

		if err := s.ackCurrentChunk(addr); err != nil {
			return nil, fmt.Errorf("ack current chunk: %v", err)

		}
		// 需要读取下一个chunk，这样我们就不会返回空响应
		s.curChunk = server.Chunk{}
		s.offset = 0
		return s.Receive(scratch)
	}

	s.offset += uint64(b.Len())
	return b.Bytes(), nil
}

func (s *Simple) updateCurrentChunk(addr string) error {
	if s.curChunk.Name != "" {
		return nil
	}

	chunks, err := s.listChunks(addr)
	if err != nil {
		return fmt.Errorf("listChunks failed: %v", err)
	}

	if len(chunks) == 0 {
		return io.EOF
	}

	// 优先考虑完整的chunk
	for _, c := range chunks {
		if c.Complete {
			s.curChunk = c
			return nil
		}
	}
	s.curChunk = chunks[0]
	return nil
}

func (s *Simple) listChunks(addr string) ([]server.Chunk, error) {
	listURL := fmt.Sprintf("%s/listChunks", addr)
	resp, err := s.cl.Get(listURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body) // 读取响应体的内容并返回错误
		if err != nil {
			return nil, err
		}

		return nil, fmt.Errorf("listChunks error: %s", body)
	}

	var res []server.Chunk
	// 创建JSON解码器并将响应体的内容解码为res
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}

	return res, nil
}

func (s *Simple) updateCurrentChunkCompleteStatus(addr string) error {
	chunks, err := s.listChunks(addr)
	if err != nil {
		return fmt.Errorf("listChunks failed: %v", err)
	}

	for _, c := range chunks {
		if c.Name == s.curChunk.Name {
			s.curChunk = c
			return nil
		}
	}

	return nil
}

func (s *Simple) ackCurrentChunk(addr string) error {
	res, err := s.cl.Get(fmt.Sprintf(addr+"/ack?chunk=%s", s.curChunk.Name))
	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, res.Body)
		return fmt.Errorf("http code %d, %s", res.StatusCode, b.String())
	}

	io.Copy(io.Discard, res.Body) // 数据丢弃
	return nil
}
