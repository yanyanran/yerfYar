package client

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net/http"
)

const defaultScratchSize = 64 * 1024

type Simple struct {
	addrs  []string
	cl     *http.Client
	offset uint64
}

func NewSimple(addrs []string) *Simple {
	return &Simple{
		addrs: addrs,
		cl:    &http.Client{},
	}
}

func (s *Simple) Send(msgs []byte) error {
	res, err := s.cl.Post(s.addrs[0]+"/write", "application/octet-stream", bytes.NewReader(msgs))
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

	addrIndex := rand.Intn(len(s.addrs))
	addr := s.addrs[addrIndex]
	readURL := fmt.Sprintf("%s/read?off=%d&maxSize=%d", addr, s.offset, len(scratch))

	res, err := s.cl.Get(readURL)
	if err != nil {
		return nil, err
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
		if err := s.ackCurrentChunk(addr); err != nil {
			return nil, err

		}
		return nil, io.EOF
	}

	s.offset += uint64(b.Len())
	return b.Bytes(), nil
}

func (s *Simple) ackCurrentChunk(addr string) error {
	res, err := s.cl.Get(addr + "/ack")
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
