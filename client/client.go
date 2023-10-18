package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/yanyanran/yerfYar/protocol"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
)

const defaultScratchSize = 64 * 1024

var errRetry = errors.New("please retry the request")

type Simple struct {
	Debug bool

	addrs    []string
	cl       *http.Client
	curChunk protocol.Chunk
	offset   uint64
}

func NewSimple(addrs []string) *Simple {
	return &Simple{
		addrs: addrs,
		cl:    &http.Client{},
	}
}

func (s *Simple) getAddr() string {
	addrIdx := rand.Intn(len(s.addrs))
	return s.addrs[addrIdx]
}

func (s *Simple) Send(category string, msgs []byte) error {
	u := url.Values{}
	u.Add("category", category)

	url := s.getAddr() + "/write?" + u.Encode()

	if s.Debug {
		log.Printf("向 %s 发送以下消息: %q", url, msgs)
	}

	res, err := s.cl.Post(url, "application/octet-stream", bytes.NewReader(msgs))
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, res.Body)
		return fmt.Errorf("http 状态码 %d, %s", res.StatusCode, b.String())
	}

	io.Copy(io.Discard, res.Body)
	return nil
}

// Process 等待新消息或出现问题时返回错误。
// scratch缓冲区用于读取数据。
// 仅当 processFn() 对于正在处理的数据没有返回错误时，读取偏移量才会提前。
func (s *Simple) Process(category string, scratch []byte, processFn func([]byte) error) error {
	if scratch == nil {
		scratch = make([]byte, defaultScratchSize)
	}

	for {
		err := s.process(category, scratch, processFn)
		if err == errRetry {
			if s.Debug {
				log.Printf("正在重试读取类别 %q ...", category)
			}
			continue
		}
		return nil
	}
}

func (s *Simple) process(category string, scratch []byte, processFn func([]byte) error) error {
	addr := s.getAddr()

	if err := s.updateCurrentChunk(category, addr); err != nil {
		return fmt.Errorf("updateCurrentChunk: %w", err)
	}

	u := url.Values{}
	u.Add("off", strconv.Itoa(int(s.offset)))
	u.Add("maxSize", strconv.Itoa(len(scratch)))
	u.Add("chunk", s.curChunk.Name)
	u.Add("category", category)

	readURL := fmt.Sprintf("%s/read?%s", addr, u.Encode())

	if s.Debug {
		log.Printf("从 %s 读取", readURL)
	}

	res, err := s.cl.Get(readURL)
	if err != nil {
		return fmt.Errorf("read %q: %v", readURL, err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, res.Body)
		return fmt.Errorf("http code %d, %s", res.StatusCode, b.String())
	}
	b := bytes.NewBuffer(scratch[0:0])
	_, err = io.Copy(b, res.Body)
	if err != nil {
		return fmt.Errorf("writing response: %v", err)
	}

	// 读0个字节但没错，意味着按照约定文件结束
	if b.Len() == 0 {
		if !s.curChunk.Complete {
			if err := s.updateCurrentChunkCompleteStatus(category, addr); err != nil {
				return fmt.Errorf("updateCurrentChunkCompleteStatus: %v", err)
			}

			if !s.curChunk.Complete {
				// 读到了最后且在请求间无新数据
				if s.offset >= s.curChunk.Size {
					return io.EOF
				}
				// 在我们发送读取请求和chunk Complete间出现了新数据
				return errRetry
			}
		}

		// 该chunk已被标记完成，但在发送读取请求和chunk Complete之间出现了新数据
		if s.offset < s.curChunk.Size {
			return errRetry
		}

		// 每读取完一个chunk就会ack一次
		if err := s.ackCurrentChunk(category, addr); err != nil {
			return fmt.Errorf("ack current chunk: %v", err)
		}
		// 需要读取下一个chunk，这样我们就不会返回空响应
		s.curChunk = protocol.Chunk{}
		s.offset = 0
		return errRetry
	}

	err = processFn(b.Bytes())
	if err == nil {
		s.offset += uint64(b.Len())
	}

	return err
}

func (s *Simple) updateCurrentChunk(category, addr string) error {
	if s.curChunk.Name != "" {
		return nil
	}

	chunks, err := s.ListChunks(category, addr)
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

func (s *Simple) updateCurrentChunkCompleteStatus(category, addr string) error {
	chunks, err := s.ListChunks(category, addr)
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

// ListChunks 返回相应yerkYar实例的chunk列表
// TODO: 将其提取到一个单独的client中
func (s *Simple) ListChunks(category, addr string) ([]protocol.Chunk, error) {
	u := url.Values{}
	u.Add("category", category)

	listURL := fmt.Sprintf("%s/listChunks?%s", addr, u.Encode())

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

	var res []protocol.Chunk
	// 创建JSON解码器并将响应体的内容解码为res
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}

	return res, nil
}

func (s *Simple) ackCurrentChunk(category, addr string) error {
	u := url.Values{}
	u.Add("chunk", s.curChunk.Name)
	u.Add("size", strconv.Itoa(int(s.offset)))
	u.Add("category", category)

	res, err := s.cl.Get(fmt.Sprintf(addr+"/ack?%s", u.Encode()))
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
