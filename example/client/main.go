package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"github.com/yanyanran/yerfYar/client"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const simpleStateFilePath = "/tmp/yerfYar-simple-example-state-%s.json"

var categoryName = flag.String("category", "stdin", "正在测试的类别")
var debug = flag.Bool("debug", false, "Debug模式")
var minSyncReplicas = flag.Uint("min-sync-replicas", 0, "写入msg时要等待的副本数")

type readResult struct {
	ln  string
	err error
}

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
	ctx, cancel := context.WithCancel(context.Background())
	log.SetFlags(log.Flags() | log.Lmicroseconds)

	addrs := []string{"http://127.0.0.1:8080", "http://127.0.0.1:8081", "http://127.0.0.1:8082", "http://127.0.0.1:8083", "http://127.0.0.1:8084"}

	cl := client.NewSimple(addrs)
	// 读持久化
	if buf, err := ioutil.ReadFile(fmt.Sprintf(simpleStateFilePath, *categoryName)); err == nil {
		if err := cl.RestoreSavedState(buf); err != nil {
			log.Printf("无法恢复保存的客户端状态: %v", err)
		}
	}
	cl.SetDebug(*debug)
	cl.SetMinSyncReplicas(*minSyncReplicas)
	fmt.Printf("在提示中输入消息以将其发送到yerkYar副本之一\n")
	go printContinuously(ctx, cl, *debug)

	rd := bufio.NewReader(os.Stdin)
	fmt.Printf("(send successful)> ")

	sigChan := make(chan os.Signal, 5)
	sigChanCopy := make(chan os.Signal, 5)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM) // 捕获os信号

	readCh := make(chan readResult)
	go func() {
		for {
			ln, err := rd.ReadString('\n')
			readCh <- readResult{ln: ln, err: err}
		}
	}()

	go func() {
		s := <-sigChan
		cancel()
		sigChanCopy <- s
	}()

	for {
		var ln string
		var err error

		select {
		case s := <-sigChanCopy:
			log.Printf("接收到信号： %v", s)
			ln = ""
			err = io.EOF
		case r := <-readCh:
			ln = r.ln
			err = r.err
		}

		if err == io.EOF {
			saveState(cl) // 持久化
			return
		} else if err != nil {
			log.Fatalf("读取失败: %v", err)
		}

		if !strings.HasSuffix(ln, "\n") {
			log.Fatalf("该行不完整: %q", ln)
		}

		if err := send(ctx, cl, ln); err != nil {
			log.Printf("向yerkYar发送数据失败: %v", err)
			fmt.Printf("(send UnSuccessful)> ")
		} else {
			fmt.Printf("(send successful)> ")
		}

		fmt.Printf("> ")
	}
}

func send(parentCtx context.Context, cl *client.Simple, ln string) error {
	ctx, cancel := context.WithTimeout(parentCtx, time.Minute+5*time.Second)
	defer cancel()

	start := time.Now()
	defer func() { log.Printf("Send() took %s", time.Since(start)) }()

	return cl.Send(ctx, *categoryName, []byte(ln))
}

func saveState(cl *client.Simple) {
	buf, err := cl.MarshalState()
	if err != nil {
		log.Printf("Marshalling client状态失败: %v", err)
	} else {
		ioutil.WriteFile(fmt.Sprintf(simpleStateFilePath, *categoryName), buf, 0666)
	}
	fmt.Println("")
}

func printContinuously(ctx context.Context, cl *client.Simple, debug bool) {
	scratch := make([]byte, 1024*1024)

	for {
		err := process(ctx, cl, scratch, func(b []byte) error {
			fmt.Printf("\n")
			log.Printf("BATCH: %s", b)
			fmt.Printf("(invitation from Process)> ")
			return nil
		})

		if err != nil && !strings.Contains(err.Error(), "context canceled") && !strings.Contains(err.Error(), "context deadline exceeded") {
			log.Printf("处理批处理时出错: %v", err)
			time.Sleep(time.Second)
		}
	}
}

func process(parentCtx context.Context, cl *client.Simple, scratch []byte, cb func(b []byte) error) error {
	ctx, cancel := context.WithTimeout(parentCtx, 10*time.Second)
	defer cancel()

	start := time.Now()
	err := cl.Process(ctx, *categoryName, scratch, cb)
	if err == nil || (!strings.Contains(err.Error(), "context canceled") && !strings.Contains(err.Error(), "context deadline exceeded")) {
		log.Printf("Process() took %s (error %v)", time.Since(start), err)
	}

	return err
}
