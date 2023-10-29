package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/yanyanran/yerfYar/client"
	"github.com/yanyanran/yerfYar/protocol"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
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

type categoryAndReplica struct {
	category string
	replica  string
}

// Client 描述复制的客户端状态并不断从其他服务器下载新chunk
type Client struct {
	logger *log.Logger

	dirname      string
	peers        []Peer
	wr           DirectWriter
	instanceName string
	httpCl       *http.Client
	r            *client.Raw

	mu                     sync.Mutex
	perCategoryPerReplica  map[categoryAndReplica]*CategoryDownloader // 支持多category
	peersAlreadyStarted    map[string]bool
	ackPeersAlreadyStarted map[string]bool
}

type CategoryDownloader struct {
	logger     *log.Logger
	eventsChan chan Chunk

	peers        []Peer
	wr           DirectWriter
	instanceName string
	httpCl       *http.Client
	r            *client.Raw

	// 正在下载的当前chunk的信息
	curMu          sync.Mutex
	curChunk       Chunk
	curChunkCancel context.CancelFunc
	curChunkDone   chan bool
}

// DirectWriter 直接写入基础存储以进行复制
type DirectWriter interface {
	Stat(category string, fileName string) (size int64, exists bool, deleted bool, err error)
	WriteDirect(category string, fileName string, contents []byte) error
	AckDirect(ctx context.Context, category string, chunk string) error
	SetReplicationDisabled(category string, v bool) error
	Write(ctx context.Context, category string, msgs []byte) (chunkName string, off int64, err error)
}

// NewCompClient 初始化复制客户端
func NewCompClient(logger *log.Logger, dirname string, wr DirectWriter, peers []Peer, instanceName string) *Client {
	httpCl := &http.Client{
		Timeout: defaultClientTimeout,
	}

	raw := client.NewRaw(httpCl)
	raw.Logger = logger

	return &Client{
		logger:                 logger,
		dirname:                dirname,
		wr:                     wr,
		instanceName:           instanceName,
		httpCl:                 httpCl,
		r:                      raw,
		peers:                  peers,
		perCategoryPerReplica:  make(map[categoryAndReplica]*CategoryDownloader),
		peersAlreadyStarted:    make(map[string]bool),
		ackPeersAlreadyStarted: make(map[string]bool),
	}
}

func (c *Client) Loop(ctx context.Context, disableAcknowledge bool) {
	if !disableAcknowledge {
		go c.replicationLoop(ctx, systemAck, c.ackPeersAlreadyStarted, c.onNewChunkInAckQueue)
	}
	c.replicationLoop(ctx, systemReplication, c.peersAlreadyStarted, c.onNewChunkInReplicationQueue)
}

// 1. 如果在 ack 前完成了chunk的下载，没问题
// 2. 如果在 ack 完成后尝试下载chunk，下载请求将失败，因为该chunk在源中已不存在（已被ack）
func (c *Client) ensureChunkIsNotBeingDownloaded(chunk Chunk) {
	c.mu.Lock()
	downloader, ok := c.perCategoryPerReplica[categoryAndReplica{
		category: chunk.Category,
		replica:  chunk.Owner,
	}]
	c.mu.Unlock()
	if !ok {
		return
	}

	downloader.curMu.Lock()
	downloadedChunk := downloader.curChunk
	cancelFunc := downloader.curChunkCancel
	doneCh := downloader.curChunkDone
	downloader.curMu.Unlock()

	if downloadedChunk.Category != chunk.Category || downloadedChunk.FileName != chunk.FileName || downloadedChunk.Owner != chunk.Owner {
		return
	}
	cancelFunc() // 取消下载
	<-doneCh     // 等待下载完成
}

func (c *Client) replicationLoop(ctx context.Context, category string, alreadyStarted map[string]bool, onChunk func(context.Context, Chunk)) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		c.startPerPeerLoops(ctx, category, c.peers, alreadyStarted, onChunk)
	}
}

func (c *Client) onNewChunkInAckQueue(ctx context.Context, ch Chunk) {
	c.logger.Printf("acknowledging chunk %+v", ch)

	c.ensureChunkIsNotBeingDownloaded(ch)

	// TODO: handle errors better
	if err := c.wr.AckDirect(ctx, ch.Category, ch.FileName); err != nil {
		c.logger.Printf("Could not ack chunk %+v from the acknowledge queue: %v", ch, err)
	}
}

func (c *Client) startPerPeerLoops(ctx context.Context, category string, peers []Peer, alreadyStarted map[string]bool, onChunk func(context.Context, Chunk)) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, peer := range peers {
		if peer.InstanceName == c.instanceName {
			continue
		} else if alreadyStarted[peer.InstanceName] {
			continue
		}

		alreadyStarted[peer.InstanceName] = true
		go c.perPeerLoop(ctx, category, peer, onChunk)
	}
}

// 用HTTP客户端从指定的peer上下载数据
func (c *Client) perPeerLoop(ctx context.Context, category string, p Peer, onChunk func(context.Context, Chunk)) {
	cl := client.NewSimple([]string{"http://" + p.ListenAddr})
	cl.SetAck(false)

	stateFilePath := filepath.Join(c.dirname, category+"-"+p.InstanceName+"-state.json")
	stateContents, err := ioutil.ReadFile(stateFilePath)
	if err == nil {
		cl.RestoreSavedState(stateContents)
	} else if !errors.Is(err, os.ErrNotExist) {
		c.logger.Printf("Could not read state file %s: %v", stateFilePath, err)
	}

	scratch := make([]byte, 256*1024)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		err := cl.Process(ctx, category, scratch, func(b []byte) error {
			d := json.NewDecoder(bytes.NewReader(b))

			for {
				var ch Chunk
				err := d.Decode(&ch)
				if errors.Is(err, io.EOF) {
					return nil
				} else if err != nil {
					c.logger.Printf("Failed to decode chunk from category %q: %v", category, err)
					continue
				}

				onChunk(ctx, ch)

				stateContents, err := cl.MarshalState()
				if err != nil {
					c.logger.Printf("Could not marshal client state: %v", err)
					continue
				}
				if err := ioutil.WriteFile(stateFilePath+".tmp", stateContents, 0666); err != nil {
					c.logger.Printf("Could not write state file %q: %v", stateFilePath+".tmp", err)
				}

				if err := os.Rename(stateFilePath+".tmp", stateFilePath); err != nil {
					c.logger.Printf("Could not rename state file %q: %v", stateFilePath, err)
				}
			}
		})

		if err != nil {
			log.Printf("无法从类别 %q 获取事件: %v", category, err)
			time.Sleep(10 * time.Second)
		}
	}
}

func (c *Client) onNewChunkInReplicationQueue(ctx context.Context, ch Chunk) {
	key := categoryAndReplica{
		category: ch.Category,
		replica:  ch.Owner,
	}

	downloader, ok := c.perCategoryPerReplica[key]
	if !ok {
		downloader = &CategoryDownloader{
			logger:       c.logger,
			eventsChan:   make(chan Chunk, 3),
			peers:        c.peers,
			wr:           c.wr,
			instanceName: c.instanceName,
			httpCl:       c.httpCl,
			r:            c.r,
		}
		go downloader.Loop(ctx)

		c.mu.Lock()
		c.perCategoryPerReplica[key] = downloader
		c.mu.Unlock()
	}

	downloader.eventsChan <- ch
}

func (c *CategoryDownloader) Loop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ch := <-c.eventsChan:
			c.downloadAllChunksUpTo(ctx, ch)
		}
	}
}

func (c *CategoryDownloader) downloadAllChunksUpTo(ctx context.Context, toReplicate Chunk) {
	for {
		err := c.downloadAllChunksUpToIteration(ctx, toReplicate)
		if err != nil {
			c.logger.Printf("对chunk %+v 进行 downloadAllChunksUpToIteration 时出错: %v", toReplicate, err)

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

	chunks, err := c.r.ListChunks(ctx, addr, toReplicate.Category, true)
	if err != nil {
		return fmt.Errorf("列出 %q 中的chunk时失败: %v", addr, err)
	}

	var chunksToReplicate []protocol.Chunk
	for _, ch := range chunks {
		instance, _ := protocol.ParseChunkFileName(ch.Name)
		// Unicode编码逐个比较字符串
		if instance == toReplicate.Owner && ch.Name <= toReplicate.FileName {
			chunksToReplicate = append(chunksToReplicate, ch)
		}
	}

	// 对筛选出的数据块按名称排序
	sort.Slice(chunksToReplicate, func(i, j int) bool {
		return chunksToReplicate[i].Name < chunksToReplicate[j].Name
	})

	for _, ch := range chunksToReplicate {
		size, exists, deleted, err := c.wr.Stat(toReplicate.Category, ch.Name)
		if err != nil {
			return fmt.Errorf("获取文件stat时出错: %v", err)
		}

		if deleted { // 不要重新下载已ack的chunk
			continue
		}

		// TODO:测试下载空块
		if !exists || ch.Size > uint64(size) || !ch.Complete || (ch.Complete && ch.Size == 0) { // 数据块不存在or不完整-> 下载
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
	c.logger.Printf("正在下载chunk %+v 中...", ch)
	defer c.logger.Printf("已完成chunk %+v 的下载", ch)

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
			c.logger.Printf("下载chunk %+v 时出现未找到错误，正在跳过chunk", ch)
			return
		} else if errors.Is(err, errIncomplete) {
			err := c.downloadChunkIteration(ctx, ch)
			if err == errIncomplete {
				time.Sleep(pollInterval)
				continue
			} else if err != nil {
				c.logger.Printf("下载chunk %+v 时出现了错误: %v", ch, err)

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
	chunkReadOffset, exists, _, err := c.wr.Stat(ch.Category, ch.FileName)
	if err != nil {
		return fmt.Errorf("获取文件信息state时出现错误: %v", err)
	}

	addr, err := c.listenAddrForChunk(ctx, ch)
	if err != nil {
		return fmt.Errorf("获取监听地址时出现错误: %v", err)
	}

	info, err := c.getChunkInfo(ctx, addr, ch)
	if err == errNotFound {
		log.Printf("在 %q 找不到chunk %+v", ch, addr)
		return nil
	} else if err != nil {
		return err
	}

	if exists && uint64(chunkReadOffset) >= info.Size {
		if !info.Complete {
			return errIncomplete
		}
		return nil
	}

	buf, err := c.downloadPart(ctx, addr, ch, chunkReadOffset)
	if err != nil {
		return fmt.Errorf("下载chunk出现错误: %w", err)
	}

	if err := c.wr.WriteDirect(ch.Category, ch.FileName, buf); err != nil {
		return fmt.Errorf("写chunk时出现错误: %v", err)
	}

	size, _, _, err := c.wr.Stat(ch.Category, ch.FileName)
	if err != nil {
		return fmt.Errorf("获取文件stat: %v", err)
	}

	if err := c.ackDownload(ctx, addr, ch, uint64(size)); err != nil {
		return fmt.Errorf("复制ack出现错误: %v", err)
	}

	if uint64(size) < info.Size || !info.Complete {
		return errIncomplete
	}

	return nil
}

func (c *CategoryDownloader) listenAddrForChunk(ctx context.Context, ch Chunk) (string, error) {
	var addr string
	for _, p := range c.peers {
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
	chunks, err := c.r.ListChunks(ctx, addr, curCh.Category, true)
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

func (c *CategoryDownloader) ackDownload(ctx context.Context, addr string, ch Chunk, size uint64) error {
	u := url.Values{}
	u.Add("size", strconv.FormatUint(size, 10))
	u.Add("chunk", ch.FileName)
	u.Add("category", ch.Category)
	u.Add("instance", c.instanceName)

	ackURL := fmt.Sprintf("%s/replication/ack?%s", addr, u.Encode())

	req, err := http.NewRequestWithContext(ctx, "GET", ackURL, nil)
	if err != nil {
		return fmt.Errorf("创建 HTTP 请求发生错误: %w", err)
	}

	resp, err := c.httpCl.Do(req)
	if err != nil {
		return fmt.Errorf("复制ack %q发生错误: %v", ackURL, err)
	}

	defer resp.Body.Close()

	var b bytes.Buffer
	_, err = io.Copy(&b, resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP 状态代码 %d，错误消息：%s", resp.StatusCode, b.Bytes())
	}

	return err
}
