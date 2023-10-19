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
	"sort"
	"strconv"
)

const defaultScratchSize = 64 * 1024

var errRetry = errors.New("please retry the request")

type ReadOffset struct {
	CurChunk          protocol.Chunk
	LastAckedChunkIdx int
	Offset            uint64
}

type state struct {
	Offsets map[string]ReadOffset
}

type Simple struct {
	Debug bool
	addrs []string
	cl    *http.Client
	st    *state
}

func NewSimple(addrs []string) *Simple {
	return &Simple{
		addrs: addrs,
		cl:    &http.Client{},
		st:    &state{Offsets: make(map[string]ReadOffset)},
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
	addr := s.getAddr()

	if len(s.st.Offsets) == 0 {
		if err := s.updateCurrentChunks(category, addr); err != nil {
			return fmt.Errorf("updateCurrentChunk: %w", err)
		}
	}

	for instance := range s.st.Offsets {
		err := s.processInstance(addr, instance, category, scratch, processFn)
		if errors.Is(err, io.EOF) {
			continue
		}
		return err
	}
	return io.EOF
}

func (s *Simple) processInstance(addr, instance, category string, scratch []byte, processFn func([]byte) error) error {
	for {
		if err := s.updateCurrentChunks(category, addr); err != nil {
			return fmt.Errorf("updateCurrentChunk: %w", err)
		}

		err := s.process(addr, instance, category, scratch, processFn)
		if err == errRetry {
			if s.Debug {
				log.Printf("正在重试读取类别 %q ...", category)
			}
			continue
		}
		return nil
	}
}

func (s *Simple) process(addr, instance, category string, scratch []byte, processFn func([]byte) error) error {
	curCh := s.st.Offsets[instance]

	u := url.Values{}
	u.Add("off", strconv.Itoa(int(curCh.Offset)))
	u.Add("maxSize", strconv.Itoa(len(scratch)))
	u.Add("chunk", curCh.CurChunk.Name)
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
		if !curCh.CurChunk.Complete {
			if err := s.updateCurrentChunkCompleteStatus(instance, category, addr); err != nil {
				return fmt.Errorf("updateCurrentChunkCompleteStatus: %v", err)
			}
			if !curCh.CurChunk.Complete {
				// 读到了最后且在请求间无新数据
				if curCh.Offset >= curCh.CurChunk.Size {
					return io.EOF
				}
				// 在我们发送读取请求和chunk Complete间出现了新数据
				return errRetry
			}
		}

		// 该chunk已被标记完成，但在发送读取请求和chunk Complete之间出现了新数据
		if curCh.Offset < curCh.CurChunk.Size {
			return errRetry
		}

		// 每读取完一个chunk就会ack一次
		if err := s.ackCurrentChunk(instance, category, addr); err != nil {
			return fmt.Errorf("ack current chunk: %v", err)
		}
		// 需要读取下一个chunk，这样我们就不会返回空响应
		_, idx := protocol.ParseChunkFileName(curCh.CurChunk.Name)
		curCh.LastAckedChunkIdx = idx
		curCh.CurChunk = protocol.Chunk{}
		curCh.Offset = 0

		s.st.Offsets[instance] = curCh
		return errRetry
	}

	err = processFn(b.Bytes())
	if err == nil {
		curCh.Offset += uint64(b.Len())
		s.st.Offsets[instance] = curCh
	}

	return err
}

func (s *Simple) updateCurrentChunks(category, addr string) error {
	chunks, err := s.ListChunks(category, addr)
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
		curChunk := s.st.Offsets[instance]
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

func (s *Simple) updateCurrentChunkCompleteStatus(instance, category, addr string) error {
	chunks, err := s.ListChunks(category, addr)
	if err != nil {
		return fmt.Errorf("listChunks failed: %v", err)
	}

	curCh := s.st.Offsets[instance]

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
			s.st.Offsets[instance] = curCh
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

func (s *Simple) ackCurrentChunk(instance, category, addr string) error {
	curCh := s.st.Offsets[instance]

	u := url.Values{}
	u.Add("chunk", curCh.CurChunk.Name)
	u.Add("size", strconv.Itoa(int(curCh.Offset)))
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
