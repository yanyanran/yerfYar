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
	"sort"
	"strconv"
	"sync"
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

	mu          sync.Mutex
	perCategory map[string]*CategoryDownloader // 支持多category
}

type CategoryDownloader struct {
	eventsCh chan Chunk

	state        *State
	wr           DirectWriter
	instanceName string
	httpCl       *http.Client
	s            *client.Simple

	// 正在下载的当前chunk的信息
	curMu          sync.Mutex
	curChunk       Chunk
	curChunkCancel context.CancelFunc
	curChunkDone   chan bool
}

// DirectWriter 直接写入基础存储以进行复制
type DirectWriter interface {
	Stat(category string, fileName string) (size int64, exists bool, err error)
	WriteDirect(category string, fileName string, contents []byte) error
	AckDirect(ctx context.Context, category string, chunk string) error
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
		s:           client.NewSimple(nil),
		perCategory: make(map[string]*CategoryDownloader),
	}
}

func (c *Client) Loop(ctx context.Context) {
	go c.ackLoop(ctx)
	c.replicationLoop(ctx)
}

func (c *Client) ackLoop(ctx context.Context) {
	for ch := range c.state.WatchAckQueue(ctx, c.instanceName) {
		log.Printf("ack chunk %+v", ch)

		c.ensureChunkIsNotBeingDownloaded(ch)

		if err := c.wr.AckDirect(ctx, ch.Category, ch.FileName); err != nil {
			log.Printf("无法从ack队列确认chunk %+v: %v", ch, err)
		}

		if err := c.state.DeleteChunkFromAckQueue(ctx, c.instanceName, ch); err != nil {
			log.Printf("无法从ack队列中删除chunk %+v： %v", ch, err)
		}
	}
}

// 1. 如果在 ack 前完成了chunk的下载，没问题
// 2. 如果在 ack 完成后尝试下载chunk，下载请求将失败，因为该chunk在源中已不存在（已被ack）
func (c *Client) ensureChunkIsNotBeingDownloaded(ch Chunk) {
	c.mu.Lock()
	downloader, ok := c.perCategory[ch.Category]
	c.mu.Unlock()
	if !ok {
		return
	}

	downloader.curMu.Lock()
	downloadedChunk := downloader.curChunk
	cancelFunc := downloader.curChunkCancel
	doneCh := downloader.curChunkDone
	downloader.curMu.Unlock()

	if downloadedChunk.Category != ch.Category || downloadedChunk.FileName != ch.FileName || downloadedChunk.Owner != ch.Owner {
		return
	}
	cancelFunc() // 取消下载
	<-doneCh     // 等待下载完成
}

func (c *Client) replicationLoop(ctx context.Context) {
	for ch := range c.state.WatchReplicationQueue(ctx, c.instanceName) {
		downloader, exist := c.perCategory[ch.Category]
		if !exist {
			downloader = &CategoryDownloader{
				eventsCh:     make(chan Chunk, 3),
				state:        c.state,
				wr:           c.wr,
				instanceName: c.instanceName,
				httpCl:       c.httpCl,
				s:            c.s,
			}
			go downloader.Loop(ctx)

			c.mu.Lock()
			c.perCategory[ch.Category] = downloader
			c.mu.Unlock()
		}
		downloader.eventsCh <- ch
	}
}

func (c *CategoryDownloader) Loop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ch := <-c.eventsCh:
			c.downloadAllChunksUpTo(ctx, ch)

			// 下载chunk成功，从复制队列删除对应的数据块
			if err := c.state.DeleteChunkFromReplicationQueue(ctx, c.instanceName, ch); err != nil {
				log.Printf("无法从复制队列中删除chunk %+v: %v", ch, err)
			}
		}
	}
}

func (c *CategoryDownloader) downloadAllChunksUpTo(ctx context.Context, toReplicate Chunk) {
	for {
		err := c.downloadAllChunksUpToIteration(ctx, toReplicate)
		if err != nil {
			log.Printf("对chunk %+v 进行 downloadAllChunksUpToIteration 时出错: %v", toReplicate, err)

			select {
			case <-ctx.Done():
				return
			default:
			}

			time.Sleep(retryTimeout)
			continue
		}

		return
	}
}

// downloadAllChunksUpTo 下载chunk，这些chunk要么是所请求的chunk，要么比所请求的chunk更旧
// 我们可以无序接收chunk复制请求，但复制必须按序进行
func (c *CategoryDownloader) downloadAllChunksUpToIteration(ctx context.Context, toReplicate Chunk) error {
	addr, err := c.listenAddrForChunk(ctx, toReplicate)
	if err != nil {
		return fmt.Errorf("获取监听端口 listenAddrForChunk 时失败: %v", err)
	}

	chunks, err := c.s.ListChunks(ctx, toReplicate.Category, addr, true)
	if err != nil {
		return fmt.Errorf("列出 %q 中的chunk时失败: %v", addr, err)
	}

	var chunksToReplicate []protocol.Chunk
	for _, ch := range chunks {
		// Unicode编码逐个比较字符串
		if ch.Name <= toReplicate.FileName {
			chunksToReplicate = append(chunksToReplicate, ch)
		}
	}

	// 对筛选出的数据块按名称排序
	sort.Slice(chunksToReplicate, func(i, j int) bool {
		return chunksToReplicate[i].Name < chunksToReplicate[j].Name
	})

	for _, ch := range chunksToReplicate {
		size, exists, err := c.wr.Stat(toReplicate.Category, ch.Name)
		if err != nil {
			return fmt.Errorf("获取文件stat时出错: %v", err)
		}

		// TODO:测试下载空块
		if !exists || ch.Size > uint64(size) || !ch.Complete { // 数据块不存在or不完整-> 下载
			c.downloadChunk(ctx, Chunk{
				Owner:    toReplicate.Owner,
				Category: toReplicate.Category,
				FileName: ch.Name,
			})
		}
	}

	return nil
}

func (c *CategoryDownloader) downloadChunk(parentCtx context.Context, ch Chunk) {
	log.Printf("正在下载chunk %+v 中...", ch)
	defer log.Printf("已完成chunk %+v 的下载", ch)

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	c.curMu.Lock()
	c.curChunk = ch
	c.curChunkCancel = cancel
	c.curChunkDone = make(chan bool)
	c.curMu.Unlock()

	defer func() {
		// 让别人知道我们不再处理这个chunk
		close(c.curChunkDone)

		c.curMu.Lock()
		c.curChunk = Chunk{}
		c.curChunkCancel = nil
		c.curChunkDone = nil
		c.curMu.Unlock()
	}()

	for {
		err := c.downloadChunkIteration(ctx, ch)
		if errors.Is(err, errNotFound) {
			log.Printf("下载chunk %+v 时出现未找到错误，正在跳过chunk", ch)
			return
		} else if errors.Is(err, errIncomplete) {
			err := c.downloadChunkIteration(ctx, ch)
			if err == errIncomplete {
				time.Sleep(pollInterval)
				continue
			} else if err != nil {
				log.Printf("下载chunk %+v 时出现了错误: %v", ch, err)

				select {
				case <-ctx.Done():
					return
				default:
				}

				// TODO 指数递减
				time.Sleep(retryTimeout)
				continue
			}
			return
		}
	}
}

func (c *CategoryDownloader) downloadChunkIteration(ctx context.Context, ch Chunk) error {
	size, _, err := c.wr.Stat(ch.Category, ch.FileName)
	if err != nil {
		return fmt.Errorf("获取文件信息state时出现错误: %v", err)
	}

	addr, err := c.listenAddrForChunk(ctx, ch)
	if err != nil {
		return fmt.Errorf("获取监听地址时出现错误: %v", err)
	}

	info, err := c.getChunkInfo(ctx, addr, ch)
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

	buf, err := c.downloadPart(ctx, addr, ch, size)
	if err != nil {
		return fmt.Errorf("下载chunk出现错误: %w", err)
	}

	if err := c.wr.WriteDirect(ch.Category, ch.FileName, buf); err != nil {
		return fmt.Errorf("写chunk时出现错误: %v", err)
	}

	size, _, err = c.wr.Stat(ch.Category, ch.FileName)
	if err != nil {
		return fmt.Errorf("获取文件stat: %v", err)
	}

	if uint64(size) < info.Size || !info.Complete {
		return errIncomplete
	}

	return nil
}

func (c *CategoryDownloader) listenAddrForChunk(ctx context.Context, ch Chunk) (string, error) {
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

func (c *CategoryDownloader) getChunkInfo(ctx context.Context, addr string, curCh Chunk) (protocol.Chunk, error) {
	chunks, err := c.s.ListChunks(ctx, curCh.Category, addr, true)
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

func (c *CategoryDownloader) downloadPart(ctx context.Context, addr string, ch Chunk, off int64) ([]byte, error) {
	u := url.Values{}
	u.Add("off", strconv.Itoa(int(off)))
	u.Add("maxSize", strconv.Itoa(batchSize))
	u.Add("chunk", ch.FileName)
	u.Add("category", ch.Category)
	u.Add("from_replication", "1")

	readURL := fmt.Sprintf("%s/read?%s", addr, u.Encode())

	req, err := http.NewRequestWithContext(ctx, "GET", readURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating http request: %w", err)
	}

	resp, err := c.httpCl.Do(req)
	if err != nil {
		return nil, fmt.Errorf("read %q: %v", readURL, err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		defer io.Copy(io.Discard, resp.Body)

		if resp.StatusCode == http.StatusNotFound {
			return nil, errNotFound
		}

		return nil, fmt.Errorf("http status code %d", resp.StatusCode)
	}

	var b bytes.Buffer
	_, err = io.Copy(&b, resp.Body)

	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}
