package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/yanyanran/yerfYar/generics/heap"
	"github.com/yanyanran/yerfYar/protocol"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var errBufTooSmall = errors.New("the buffer is too small to contain a single message")

const DeletedSuffix = ".deleted"

type StorageHooks interface {
	AfterCreatingChunk(ctx context.Context, category string, fileName string) error
	AfterAcknowledgeChunk(ctx context.Context, category string, fileName string) error
}

type downloadNotice struct {
	chunk string
	size  uint64
}

type replicationSub struct {
	chunk   string
	size    uint64
	channel chan bool
}

func (r replicationSub) Less(v replicationSub) bool {
	if r.chunk == v.chunk {
		return r.size < v.size
	}
	return r.chunk < v.chunk
}

type OnDisk struct {
	logger *log.Logger

	dirname      string
	category     string
	instanceName string
	maxChunkSize uint64

	repl StorageHooks

	downloadNoticeMu         sync.RWMutex
	downloadNotificationSubs heap.Min[replicationSub]
	downloadNoticeMap        map[string]*downloadNotice

	writeMu                     sync.Mutex
	lastChunkFp                 *os.File
	lastChunk                   string
	lastChunkSize               uint64
	lastChunkIdx                uint64
	lastChunkAddedToReplication bool

	fpsMu sync.Mutex
	fps   map[string]*os.File // 缓存已打开的文件描述符，以便在需要读或写文件块时可快速访问
}

func NewOnDisk(logger *log.Logger, dirname, category, instanceName string, maxChunkSize uint64, rotateChunkInterval time.Duration, repl StorageHooks) (*OnDisk, error) {
	s := &OnDisk{
		logger:                   logger,
		dirname:                  dirname,
		category:                 category,
		instanceName:             instanceName,
		repl:                     repl,
		maxChunkSize:             maxChunkSize,
		downloadNoticeMap:        make(map[string]*downloadNotice),
		downloadNotificationSubs: heap.NewMin[replicationSub](),
	}

	if err := s.initLastChunkIdx(dirname); err != nil {
		return nil, err
	}
	go s.createNextChunkThread(rotateChunkInterval)
	return s, nil
}

func (s *OnDisk) initLastChunkIdx(dirname string) error {
	files, err := os.ReadDir(dirname) // ...chunk
	if err != nil {
		return fmt.Errorf("readdir(%q): %v", dirname, err)
	}

	for _, fi := range files {
		instance, chunkIdx := protocol.ParseChunkFileName(fi.Name())
		if chunkIdx < 0 || instance != s.instanceName {
			continue
		}

		if uint64(chunkIdx)+1 >= s.lastChunkIdx {
			s.lastChunkIdx = uint64(chunkIdx) + 1
		}
	}

	return nil
}

// WriteDirect 直接写入chunk文件以避免复制的循环依赖（非线程安全）
func (s *OnDisk) WriteDirect(chunk string, contents []byte) error {
	filename := filepath.Join(s.dirname, chunk)
	fp, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer fp.Close()

	_, err = fp.Write(contents)
	return err
}

func (s *OnDisk) createNextChunkThread(interval time.Duration) {
	for {
		time.Sleep(interval)

		if err := s.tryCreateNextEmptyChunkIfNeeded(); err != nil {
			s.logger.Printf("在后台创建下一个空chunk失败: %v", err)
		}
	}
}

func (s *OnDisk) tryCreateNextEmptyChunkIfNeeded() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.lastChunkSize == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.logger.Printf("从timer创建空chunk")

	return s.createNextChunk(ctx)
}

func (s *OnDisk) createNextChunk(ctx context.Context) error {
	if s.lastChunkFp != nil {
		s.lastChunkFp.Close() // 关闭上一个chunk的fp（确保在创建新chunk前关闭旧chunk）
		s.lastChunkFp = nil
	}

	newChunk := fmt.Sprintf("%s-chunk%09d", s.instanceName, s.lastChunkIdx)

	fp, err := os.OpenFile(filepath.Join(s.dirname, newChunk), os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer fp.Close()

	s.lastChunk = newChunk
	s.lastChunkSize = 0
	s.lastChunkIdx++
	s.lastChunkAddedToReplication = false

	if err := s.repl.AfterCreatingChunk(ctx, s.category, s.lastChunk); err != nil {
		return fmt.Errorf("创建新chunk后发生错误: %w", err)
	}

	s.lastChunkAddedToReplication = true
	return nil
}

func (s *OnDisk) getLastChunkFp() (*os.File, error) {
	if s.lastChunkFp != nil {
		return s.lastChunkFp, nil
	}

	fp, err := os.OpenFile(filepath.Join(s.dirname, s.lastChunk), os.O_WRONLY, 0666)
	if err != nil {
		return nil, fmt.Errorf("用于写入的chunk发生错误: %v", err)
	}
	s.lastChunkFp = fp

	return fp, nil
}

// Write 接受来自客户端的消息并存储
func (s *OnDisk) Write(ctx context.Context, msgs []byte) (chunkName string, offset int64, err error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	willExceedMaxChunkSize := s.lastChunkSize+uint64(len(msgs)) > s.maxChunkSize

	if s.lastChunk == "" || (s.lastChunkSize > 0 && willExceedMaxChunkSize) {
		if err := s.createNextChunk(ctx); err != nil {
			return "", 0, fmt.Errorf("创建下一个chunk失败 %v", err)
		}
	}

	if !s.lastChunkAddedToReplication {
		if err := s.repl.AfterCreatingChunk(ctx, s.category, s.lastChunk); err != nil {
			return "", 0, fmt.Errorf("创建新chunk后发生错误: %w", err)
		}
	}

	fp, err := s.getLastChunkFp()
	if err != nil {
		return "", 0, err
	}

	n, err := fp.Write(msgs)
	if err != nil {
		// 【回退机制】写入失败后尝试截断到原本chunk的大小来恢复
		if truncateErr := fp.Truncate(int64(s.lastChunkSize)); truncateErr != nil {
			s.logger.Printf("无法截断 %s: %v", fp.Name(), err)
		}
		return "", 0, err
	}

	s.lastChunkSize += uint64(n)
	return s.lastChunk, int64(s.lastChunkSize), nil
}

// Wait 等待直到minSyncReplicas报告成功下载了相应的chunk
func (s *OnDisk) Wait(ctx context.Context, chunkName string, offset uint64, minSyncReplicas uint) error {
	r := replicationSub{
		chunk:   chunkName,
		size:    offset,
		channel: make(chan bool, 2),
	}

	s.downloadNoticeMu.Lock()
	s.downloadNotificationSubs.Push(r)
	s.downloadNoticeMu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.channel:
		}
		var replicatedCount uint

		s.downloadNoticeMu.RLock()
		for _, n := range s.downloadNoticeMap {
			// 等待ack对象版本比接收ack对象的版本高/= -> download成功
			if (n.chunk > chunkName) || (n.chunk == chunkName && n.size >= offset) {
				replicatedCount++
			}
		}
		s.downloadNoticeMu.RUnlock()

		if replicatedCount >= minSyncReplicas {
			return nil
		}
	}
}

// Read 读文件指定offset到byte切片中，然后写到对应writer中去
func (s *OnDisk) Read(chunk string, offset uint64, maxSize uint64, w io.Writer) error {
	chunk = filepath.Clean(chunk)
	_, err := os.Stat(filepath.Join(s.dirname, chunk))
	if err != nil {
		return fmt.Errorf("stat %q: %w", chunk, err)
	}

	fp, err := os.Open(filepath.Join(s.dirname, chunk))
	if err != nil {
		return fmt.Errorf("open(%q)发生错误: %w", chunk, err)
	}
	defer fp.Close()

	buf := make([]byte, maxSize)
	n, err := fp.ReadAt(buf, int64(offset))

	if n == 0 {
		if err == io.EOF { // 已到达文件末尾
			return nil
		} else if err != nil {
			return err
		}
	}

	// 如果最后一条消息被切成两半，则不发送不完整部分
	truncated, _, err := cutToLastMessage(buf[0:n])
	if err != nil {
		return err
	}
	if _, err := w.Write(truncated); err != nil {
		return err
	}
	return nil
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

func (s *OnDisk) isLastChunk(chunk string) bool {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	return chunk == s.lastChunk
}

// Ack 将当前消息块标记为完成并删除其内容
func (s *OnDisk) Ack(ctx context.Context, chunk string, size uint64) error {
	if s.isLastChunk(chunk) {
		return fmt.Errorf("could not delete incomplete chunk %q", chunk)
	}

	chunkFilename := filepath.Join(s.dirname, chunk)

	fi, err := os.Stat(chunkFilename)
	if err != nil {
		return fmt.Errorf("stat %q: %w", chunk, err)
	}

	if uint64(fi.Size()) > size {
		return fmt.Errorf("文件未完全处理：提供的已处理大小 %d 小于 Chunk 文件大小 %d", size, fi.Size())
	}

	if err := s.doAckChunk(chunk); err != nil {
		return fmt.Errorf("ack %q: %v", chunk, err)
	}

	if err := s.repl.AfterAcknowledgeChunk(ctx, s.category, chunk); err != nil {
		s.logger.Printf("无法复制ack请求: %v", err)
	}
	return nil
}

func (s *OnDisk) doAckChunk(chunk string) error {
	chunkFilename := filepath.Join(s.dirname, chunk)

	fp, err := os.OpenFile(chunkFilename, os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("无法获取chunk %q 的ack操作的文件描述符：%v", chunk, err)
	}
	defer fp.Close()

	if err := fp.Truncate(0); err != nil {
		return fmt.Errorf("未能截断文件 %q: %v", chunk, err)
	}

	if err := os.Rename(chunkFilename, chunkFilename+DeletedSuffix); err != nil {
		return fmt.Errorf("无法将文件 %q 重命名为已删除形式：%v", chunk, err)
	}

	return nil
}

func (s *OnDisk) AckDirect(chunk string) error {
	if err := s.doAckChunk(chunk); err != nil && !errors.Is(err, os.ErrNotExist) { // ack文件不存在时不警告
		return fmt.Errorf("removing %q: %v", chunk, err)
	}
	return nil
}

// ReplicationAck 让正在等待min_sync_replicas的写入程序知道其写入成功
func (s *OnDisk) ReplicationAck(ctx context.Context, chunk, instance string, size uint64) error {
	s.downloadNoticeMu.Lock()
	defer s.downloadNoticeMu.Unlock()

	s.downloadNoticeMap[instance] = &downloadNotice{
		chunk: chunk,
		size:  size,
	}

	for s.downloadNotificationSubs.Len() > 0 {
		r := s.downloadNotificationSubs.Pop()

		// 等待ack对象版本比接收ack对象的版本高/= -> download成功
		if (chunk == r.chunk && size >= r.size) || chunk > r.chunk {
			select {
			case r.channel <- true:
			default:
			}
		} else {
			s.downloadNotificationSubs.Push(r)
			break
		}
	}
	return nil
}

func (s *OnDisk) ListChunks() ([]protocol.Chunk, error) {
	var res []protocol.Chunk

	dis, err := os.ReadDir(s.dirname)
	if err != nil {
		return nil, err
	}

	for idx, di := range dis {
		fi, err := di.Info()
		if errors.Is(err, os.ErrNotExist) { // dir不存在
			continue
		} else if err != nil {
			return nil, fmt.Errorf("reading directory: %v", err)
		}

		if strings.HasSuffix(di.Name(), DeletedSuffix) { // 检查di.Name()是否以DeletedSuffix结尾
			continue
		}

		instanceName, _ := protocol.ParseChunkFileName(di.Name())

		c := protocol.Chunk{
			Name:     di.Name(),
			Complete: true,
			Size:     uint64(fi.Size()),
		}

		if idx == len(dis)-1 { // last chunk
			c.Complete = false
		} else if nextInstance, _ := protocol.ParseChunkFileName(dis[idx+1].Name()); nextInstance != instanceName {
			c.Complete = false
		}

		res = append(res, c)
	}

	return res, nil
}
