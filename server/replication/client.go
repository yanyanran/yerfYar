package replication

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/yanyanran/yerfYar/client"
	"github.com/yanyanran/yerfYar/protocol"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const defaultClientTimeout = 10 * time.Second
const pollInterval = 50 * time.Millisecond
const retryTimeout = 1 * time.Second

const batchSize = 4 * 1024 * 1024 // 4 MB

var errNotFound = errors.New("chunk not found")
var errIncomplete = errors.New("chunk is not complete")

// Client 描述复制的客户端状态并不断从其他服务器下载新chunk
type Client struct {
	state        *State
	wr           DirectWriter
	instanceName string
	httpCl       *http.Client
	s            *client.Simple
}

// DirectWriter 直接写入基础存储以进行复制
type DirectWriter interface {
	Stat(category string, fileName string) (size int64, exists bool, err error)
	WriteDirect(category string, fileName string, contents []byte) error
}

// NewCompClient 初始化复制客户端
func NewCompClient(st *State, wr DirectWriter, instanceName string) *Client {
	return &Client{
		state:        st,
		wr:           wr,
		instanceName: instanceName,
		httpCl: &http.Client{
			Timeout: defaultClientTimeout,
		},
		s: client.NewSimple(nil),
	}
}

func (c *Client) Loop(ctx context.Context) {
	for ch := range c.state.WatchReplicationQueue(ctx, c.instanceName) {
		c.downloadChunk(ch)

		// TODO: 处理错误
		if err := c.state.DeleteChunkFromReplicationQueue(ctx, c.instanceName, ch); err != nil {
			log.Printf("无法从复制队列中删除chunk %+v: %v", ch, err)
		}
	}
}

func (c *Client) downloadChunk(ch Chunk) {
	log.Printf("正在下载chunk %+v 中...", ch)
	defer log.Printf("已完成chunk %+v 的下载", ch)

	for {
		err := c.downloadChunkIteration(ch)
		if err == errIncomplete {
			time.Sleep(pollInterval)
			continue
		} else if err != nil {
			log.Printf("下载chunk %+v 时出现了错误: %v", ch, err)
			time.Sleep(retryTimeout)
			continue
		}
		return
	}
}

func (c *Client) downloadChunkIteration(ch Chunk) error {
	size, _, err := c.wr.Stat(ch.Category, ch.FileName)
	if err != nil {
		return fmt.Errorf("获取文件信息state时出现错误: %v", err)
	}

	addr, err := c.listenAddrForChunk(ch)
	if err != nil {
		return fmt.Errorf("获取监听地址时出现错误: %v", err)
	}

	info, err := c.getChunkInfo(addr, ch)
	if err == errNotFound {
		log.Printf("在 %q 找不到chunk", addr)
		return nil
	} else if err != nil {
		return err
	}

	if uint64(size) >= info.Size {
		if !info.Complete {
			return errIncomplete
		}
		return nil
	}

	buf, err := c.downloadPart(addr, ch, size)
	if err != nil {
		return fmt.Errorf("下载chunk出现错误: %v", err)
	}

	if err := c.wr.WriteDirect(ch.Category, ch.FileName, buf); err != nil {
		return fmt.Errorf("写chunk时出现错误: %v", err)
	}

	if !info.Complete {
		return errIncomplete
	}

	return nil
}

func (c *Client) listenAddrForChunk(ch Chunk) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultClientTimeout)
	defer cancel()

	peers, err := c.state.ListPeers(ctx)
	if err != nil {
		return "", err
	}

	var addr string
	for _, p := range peers {
		if p.InstanceName == ch.Owner {
			addr = p.ListenAddr
			break
		}
	}

	if addr == "" {
		return "", fmt.Errorf("找不到peer %q", ch.Owner)
	}

	return "http://" + addr, nil
}

func (c *Client) getChunkInfo(addr string, curCh Chunk) (protocol.Chunk, error) {
	chunks, err := c.s.ListChunks(curCh.Category, addr)
	if err != nil {
		return protocol.Chunk{}, err
	}

	for _, ch := range chunks {
		if ch.Name == curCh.FileName {
			return ch, nil
		}
	}

	return protocol.Chunk{}, errNotFound
}

func (c *Client) downloadPart(addr string, ch Chunk, off int64) ([]byte, error) {
	u := url.Values{}
	u.Add("off", strconv.Itoa(int(off)))
	u.Add("maxSize", strconv.Itoa(batchSize))
	u.Add("chunk", ch.FileName)
	u.Add("category", ch.Category)

	readURL := fmt.Sprintf("%s/read?%s", addr, u.Encode())

	resp, err := c.httpCl.Get(readURL)
	if err != nil {
		return nil, fmt.Errorf("read %q: %v", readURL, err)
	}

	defer resp.Body.Close()

	var b bytes.Buffer
	_, err = io.Copy(&b, resp.Body)

	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}
