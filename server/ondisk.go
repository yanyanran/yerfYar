package server

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/yanyanran/yerfYar/protocal"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
)

// TODO: 限制msg的最大大小
const maxFileChunkSize = 20 * 1024 * 1024 // bytes
var errBufTooSmall = errors.New("the buffer is too small to contain a single message")
var filenameRegexp = regexp.MustCompile("^chunk([0-9]+)$") // 匹配以"chunk"开头，后跟一个或多个数字的字符串

type OnDisk struct {
	dirname string

	writeMu       sync.Mutex
	lastChunk     string
	lastChunkSize uint64
	lastChunkIdx  uint64

	fpsMu sync.Mutex
	fps   map[string]*os.File // 缓存已打开的文件描述符，以便在需要读或写文件块时可快速访问
}

func NewOnDisk(dirname string) (*OnDisk, error) {
	s := &OnDisk{
		dirname: dirname,
		fps:     make(map[string]*os.File),
	}

	if err := s.initLastChunkIdx(dirname); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *OnDisk) initLastChunkIdx(dirname string) error {
	files, err := os.ReadDir(dirname) // ...chunk
	if err != nil {
		return fmt.Errorf("readdir(%q): %v", dirname, err)
	}

	for _, fi := range files {
		res := filenameRegexp.FindStringSubmatch(fi.Name()) // ...chunk1 chunk2...
		if res == nil {
			continue
		}

		chunkIdx, err := strconv.Atoi(res[1]) // ->int
		if err != nil {
			return fmt.Errorf("unexpected error parsing filename %q: %v", fi.Name(), err)
		}

		if uint64(chunkIdx)+1 >= s.lastChunkIdx {
			s.lastChunkIdx = uint64(chunkIdx) + 1
		}
	}

	return nil
}

func (s *OnDisk) Write(msgs []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.lastChunk == "" || (s.lastChunkSize+uint64(len(msgs)) > maxFileChunkSize) {
		s.lastChunk = fmt.Sprintf("chunk%09d", s.lastChunkIdx)
		s.lastChunkSize = 0
		s.lastChunkIdx++
	}

	fp, err := s.getFileDescriptor(s.lastChunk, true)
	if err != nil {
		return err
	}

	_, err = fp.Write(msgs)
	s.lastChunkSize += uint64(len(msgs))
	return err
}

// 检查给定chunk的文件描述符是否已映射
func (s *OnDisk) getFileDescriptor(chunk string, write bool) (*os.File, error) {
	s.fpsMu.Lock()
	defer s.fpsMu.Unlock()

	fp, ok := s.fps[chunk]
	if ok {
		return fp, nil
	}

	fl := os.O_RDONLY
	if write {
		fl = os.O_CREATE | os.O_RDWR | os.O_EXCL
	}

	filename := filepath.Join(s.dirname, chunk)
	fp, err := os.OpenFile(filename, fl, 0666)
	if err != nil {
		return nil, fmt.Errorf("create file %q: %s", filename, err)
	}

	s.fps[chunk] = fp
	return fp, nil
}

func (s *OnDisk) forgetFileDescriptor(chunk string) {
	s.fpsMu.Lock()
	defer s.fpsMu.Unlock()

	fp, ok := s.fps[chunk]
	if !ok {
		return
	}

	fp.Close()
	delete(s.fps, chunk)
}

// Read 读文件指定offset到byte切片中，然后写到对应writer中去
func (s *OnDisk) Read(chunk string, offset uint64, maxSize uint64, w io.Writer) error {
	chunk = filepath.Clean(chunk)
	_, err := os.Stat(filepath.Join(s.dirname, chunk))
	if err != nil {
		return fmt.Errorf("stat %q: %w", chunk, err)
	}

	fp, err := s.getFileDescriptor(chunk, false)
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
func (s *OnDisk) Ack(chunk string, size uint64) error {
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

	if err := os.Remove(chunkFilename); err != nil {
		return fmt.Errorf("removing %q: %v", chunk, err)
	}

	s.forgetFileDescriptor(chunk)
	return nil
}

func (s *OnDisk) ListChunks() ([]protocal.Chunk, error) {
	var res []protocal.Chunk

	dis, err := os.ReadDir(s.dirname)
	if err != nil {
		return nil, err
	}

	for _, di := range dis {
		fi, err := di.Info()
		if errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, fmt.Errorf("reading directory: %v", err)
		}

		c := protocal.Chunk{
			Name:     di.Name(),
			Complete: di.Name() != s.lastChunk,
			Size:     uint64(fi.Size()),
		}
		res = append(res, c)
	}

	return res, nil
}
