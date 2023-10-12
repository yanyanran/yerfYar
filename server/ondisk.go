package server

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// TODO: 限制msg的最大大小
const readBlockSize = 1024 * 1024
const maxFileChunkSize = 20 * 1024 * 1024 // bytes

type OnDisk struct {
	dirname string

	sync.RWMutex
	lastChunk     string
	lastChunkSize uint64
	lastChunkIdx  uint64
	fps           map[string]*os.File
}

func NewOnDisk(dirname string) *OnDisk {
	return &OnDisk{
		dirname: dirname,
		fps:     make(map[string]*os.File),
	}
}

func (s *OnDisk) Write(msgs []byte) error {
	s.Lock()
	defer s.Unlock()

	if s.lastChunk == "" || (s.lastChunkSize+uint64(len(msgs)) > maxFileChunkSize) {
		s.lastChunk = fmt.Sprintf("chunk%d", s.lastChunkIdx)
		s.lastChunkSize = 0
		s.lastChunkIdx++
	}

	fp, err := s.getFileDescriptor(s.lastChunk)
	if err != nil {
		return err
	}

	_, err = fp.Write(msgs)
	s.lastChunkSize += uint64(len(msgs))
	return err
}

// 检查给定chunk的文件描述符是否已映射
func (s *OnDisk) getFileDescriptor(chunk string) (*os.File, error) {
	fp, ok := s.fps[chunk]
	if ok {
		return fp, nil
	}

	fp, err := os.OpenFile(filepath.Join(s.dirname, chunk), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("create file %q: %s", fp.Name(), err)
	}

	s.fps[chunk] = fp
	return fp, nil
}

// Read 读文件指定offset到byte切片中，然后写到对应writer中去
func (s *OnDisk) Read(chunk string, offset uint64, maxSize uint64, w io.Writer) error {
	s.Lock()
	defer s.Unlock()

	chunk = filepath.Clean(chunk)
	_, err := os.Stat(filepath.Join(s.dirname, chunk))
	if err != nil {
		return fmt.Errorf("stat %q: %w", chunk, err)
	}

	fp, err := s.getFileDescriptor(chunk)
	if err != nil {
		return fmt.Errorf("getFileDescriptor(%q): %v", chunk, err)
	}

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

// Ack 将当前消息块标记为完成并删除其内容
func (s *OnDisk) Ack(chunk string) error {
	s.Lock()
	defer s.Unlock()

	if chunk == s.lastChunk {
		return fmt.Errorf("could not delete incomplete chunk %q", chunk)
	}

	chunkFilename := filepath.Join(s.dirname, chunk)

	_, err := os.Stat(chunkFilename)
	if err != nil {
		return fmt.Errorf("stat %q: %w", chunk, err)
	}

	if err := os.Remove(chunkFilename); err != nil {
		return fmt.Errorf("removing %q: %v", chunk, err)
	}

	fp, ok := s.fps[chunk]
	if ok {
		fp.Close()
	}
	delete(s.fps, chunk)
	return nil
}

func (s *OnDisk) ListChunks() ([]Chunk, error) {
	var res []Chunk

	dis, err := os.ReadDir(s.dirname)
	if err != nil {
		return nil, err
	}

	for _, di := range dis {
		c := Chunk{
			Name:     di.Name(),
			Complete: di.Name() != s.lastChunk,
		}
		res = append(res, c)
	}

	return res, nil
}
