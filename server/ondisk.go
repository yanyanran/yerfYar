package server

import (
	"io"
	"os"
)

// TODO: 限制msg的最大大小
const readBlockSize = 1024 * 1024

type OnDisk struct {
	fp *os.File
}

func NewOnDisk(fp *os.File) *OnDisk {
	return &OnDisk{fp: fp}
}

func (s *OnDisk) Write(msgs []byte) error {
	_, err := s.fp.Write(msgs)
	return err
}

// Read 读文件指定offset到byte切片中，然后写到对应writer中去
func (s *OnDisk) Read(offset uint64, maxSize uint64, w io.Writer) error {
	buf := make([]byte, maxSize)
	n, err := s.fp.ReadAt(buf, int64(offset))

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
func (s *OnDisk) Ack() error {
	var err error

	err = s.fp.Truncate(0)
	if err != nil {
		return err
	}

	// 从上一个地方继续读
	_, err = s.fp.Seek(0, 0)
	return err
}
