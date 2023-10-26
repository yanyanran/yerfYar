package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"

	"github.com/yanyanran/yerfYar/protocol"
)

// Raw yerfYar的HTTP客户端，允许用户使用它的API向yerfYar服务器发送请求
type Raw struct {
	debug  bool
	Logger *log.Logger

	cl *http.Client
}

func NewRaw(cl *http.Client) *Raw {
	if cl == nil {
		cl = &http.Client{}
	}

	return &Raw{
		cl: cl,
	}
}

// SetDebug 启用或禁用客户端的调试日志记录
func (r *Raw) SetDebug(v bool) {
	r.debug = v
}

func (r *Raw) logger() *log.Logger {
	if r.Logger == nil {
		return log.Default()
	}

	return r.Logger
}

// Write 将事件发送到相应的yerfYar服务器
func (r *Raw) Write(ctx context.Context, addr string, category string, msgs []byte) error {
	u := url.Values{}
	u.Add("category", category)

	url := addr + "/write?" + u.Encode()

	if r.debug {
		debugMsgs := msgs
		if len(debugMsgs) > 128 {
			debugMsgs = []byte(fmt.Sprintf("%s...（总共 %d 字节）", msgs[0:128], len(msgs)))
		}
		r.logger().Printf("向 %s 发送以下消息: %q", url, debugMsgs)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(msgs))
	if err != nil {
		return fmt.Errorf("发起HTTP请求发生错误: %v", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := r.cl.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, resp.Body)
		return fmt.Errorf("http 状态码 %d, %s", resp.StatusCode, b.String())
	}

	io.Copy(ioutil.Discard, resp.Body)
	return nil
}

func (r *Raw) Read(ctx context.Context, addr string, category string, chunk string, off uint64, scratch []byte) (res []byte, found bool, err error) {
	u := url.Values{}
	u.Add("off", strconv.FormatInt(int64(off), 10))
	u.Add("maxSize", strconv.Itoa(len(scratch)))
	u.Add("chunk", chunk)
	u.Add("category", category)

	readURL := fmt.Sprintf("%s/read?%s", addr, u.Encode())

	if r.debug {
		r.logger().Printf("从 %s 读取", readURL)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", readURL, nil)
	if err != nil {
		return nil, false, fmt.Errorf("creating Request: %v", err)
	}

	resp, err := r.cl.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("read %q: %v", readURL, err)
	}

	defer resp.Body.Close()

	b := bytes.NewBuffer(scratch[0:0])
	_, err = io.Copy(b, resp.Body)
	if err != nil {
		return nil, false, fmt.Errorf("read %q: %v", readURL, err)
	}

	// 如果chunk不存在但通过ListChunks()获得了它，则意味着该chunk尚未复制到该服务器
	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	} else if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("GET %q: http code %d, %s", readURL, resp.StatusCode, b.String())
	}

	return b.Bytes(), true, nil
}

// ListChunks 返回相应yerfYar实例的chunk列表
func (r *Raw) ListChunks(ctx context.Context, addr, category string, fromReplication bool) ([]protocol.Chunk, error) {
	u := url.Values{}
	u.Add("category", category)
	if fromReplication {
		u.Add("from_replication", "1")
	}

	listURL := fmt.Sprintf("%s/listChunks?%s", addr, u.Encode())

	req, err := http.NewRequestWithContext(ctx, "GET", listURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating new http request: %w", err)
	}

	resp, err := r.cl.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body) // 读取响应体的内容并返回错误
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

	if r.debug {
		r.logger().Printf("ListChunks(%q) 返回 %+v", addr, res)
	}

	return res, nil
}

func (r *Raw) Ack(ctx context.Context, addr, category, chunk string, size uint64) error {
	u := url.Values{}
	u.Add("chunk", chunk)
	u.Add("size", strconv.FormatInt(int64(size), 10))
	u.Add("category", category)

	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf(addr+"/ack?%s", u.Encode()), nil)
	if err != nil {
		return fmt.Errorf("创建 Request: %v", err)
	}

	resp, err := r.cl.Do(req)
	if err != nil {
		return fmt.Errorf("正在执行 request: %v", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, resp.Body)
		return fmt.Errorf("http 状态码 %d, %s", resp.StatusCode, b.String())
	}

	io.Copy(ioutil.Discard, resp.Body) // 数据丢弃
	return nil
}
