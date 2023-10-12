package main

import (
	"errors"
	"fmt"
	"github.com/yanyanran/yerfYar/client"
	"go/build"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const (
	maxN          = 10000000
	maxBufferSize = 1024 * 1024

	sendFmt = "Send: net %13s, cpu %13s (%.1f MiB)"
	recvFmt = "Recv: net %13s, cpu %13s"
)

type sumAndErr struct {
	sum int64
	err error
}

func main() {
	if err := runTest(); err != nil {
		log.Fatalf("Test failed: %v", err)
	}

	log.Printf("Test passed!")
}

// 更细粒度的测试
func runTest() error {
	log.SetFlags(log.Flags() | log.Lmicroseconds)

	goPath := os.Getenv("GOPATH")
	if goPath == "" {
		goPath = build.Default.GOPATH
	}

	log.Printf("编译yerfYar....")
	out, err := exec.Command("go", "install", "-v", "github.com/yanyanran/yerfYar").CombinedOutput()
	if err != nil {
		log.Printf("Failed to build: %v", err)
		return fmt.Errorf("compilation failed: %v (out: %s)", err, string(out))
	}

	// TODO: 随机端口
	port := 8080 // "test" in l33t

	// TODO: 使数据库路径随机
	dbPath := "/tmp/yerfYar"
	os.RemoveAll(dbPath)
	os.Mkdir(dbPath, 0777)

	ioutil.WriteFile("/tmp/yerfYar/chunk1", []byte("12345\n"), 0666)

	log.Printf("Running yerfYar on port %d", port)

	cmd := exec.Command(goPath+"/bin/yerfYar", "-dirname="+dbPath, fmt.Sprintf("-port=%d", port))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Start()
	if err != nil {
		return err
	}
	defer cmd.Process.Kill()

	log.Printf("Waiting for the port localhost:%d to open", port)
	for i := 0; i <= 100; i++ {
		timeout := time.Millisecond * 50
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("localhost", fmt.Sprint(port)), timeout)
		if err != nil {
			time.Sleep(timeout)
			continue
		}
		conn.Close()
		break
	}

	log.Printf("Starting the test")

	s := client.NewSimple([]string{fmt.Sprintf("http://localhost:%d", port)})

	want, got, err := sendAndReceiveConcurrently(s)
	if err != nil {
		return err
	}

	want += 12345 // 已存在的chunk的内容
	if want != got {
		return fmt.Errorf("the expected sum %d is not equal to the actual sum %d (delivered %1.f%%)", want, got, (float64(got)/float64(want))*100)
	}

	return nil
}

// 并发发送和接收
func sendAndReceiveConcurrently(s *client.Simple) (want, got int64, err error) {
	wantCh := make(chan sumAndErr, 1)
	gotCh := make(chan sumAndErr, 1)
	sendFinishedCh := make(chan bool, 1) // 通知管道

	go func() {
		want, err := send(s)
		log.Printf("Send finished")

		wantCh <- sumAndErr{
			sum: want,
			err: err,
		}
		sendFinishedCh <- true
	}()

	go func() {
		got, err := receive(s, sendFinishedCh)
		gotCh <- sumAndErr{
			sum: got,
			err: err,
		}
	}()

	wantRes := <-wantCh
	if wantRes.err != nil {
		return 0, 0, fmt.Errorf("send: %v", wantRes.err)
	}

	gotRes := <-gotCh
	if gotRes.err != nil {
		return 0, 0, fmt.Errorf("receive: %v", gotRes.err)
	}

	return wantRes.sum, gotRes.sum, err
}

func send(s *client.Simple) (sum int64, err error) {
	sendStart := time.Now()
	var networkTime time.Duration
	var sentBytes int

	defer func() {
		log.Printf(sendFmt, networkTime, time.Since(sendStart)-networkTime, float64(sentBytes)/1024/1024)
	}()

	buf := make([]byte, 0, maxBufferSize)

	for i := 0; i <= maxN; i++ {
		sum += int64(i)

		buf = strconv.AppendInt(buf, int64(i), 10)
		buf = append(buf, '\n')

		if len(buf) >= maxBufferSize {
			start := time.Now()
			if err := s.Send(buf); err != nil {
				return 0, err
			}
			networkTime += time.Since(start)
			sentBytes += len(buf)

			buf = buf[0:0]
		}
	}

	if len(buf) != 0 {
		start := time.Now()
		if err := s.Send(buf); err != nil {
			return 0, err
		}
		networkTime += time.Since(start)
		sentBytes += len(buf)
	}

	return sum, nil
}

func receive(s *client.Simple, sendFinishedCh chan bool) (sum int64, err error) {
	buf := make([]byte, maxBufferSize)

	var parseTime time.Duration
	receiveStart := time.Now()
	defer func() {
		log.Printf(recvFmt, time.Since(receiveStart)-parseTime, parseTime)
	}()

	trimNL := func(r rune) bool { return r == '\n' }

	sendFinished := false
	for {
		select {
		case <-sendFinishedCh:
			log.Printf("Receive: got information that send finished")
			sendFinished = true
		default:
		}

		res, err := s.Receive(buf)
		if errors.Is(err, io.EOF) { // 接收操作已完成
			if sendFinished {
				// send已完成
				return sum, nil
			}

			time.Sleep(time.Millisecond * 10)
			continue
		} else if err != nil {
			return 0, err
		}

		start := time.Now()

		ints := strings.Split(strings.TrimRightFunc(string(res), trimNL), "\n")
		for _, str := range ints {
			i, err := strconv.Atoi(str)
			if err != nil {
				return 0, err
			}

			sum += int64(i)
		}

		parseTime += time.Since(start)
	}
}
