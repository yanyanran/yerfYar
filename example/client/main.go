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
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const simpleStateFilePath = "/tmp/yerfYar-simple-example-state-%s.json"

var categoryName = flag.String("category", "stdin", "正在测试的类别")

type readResult struct {
	ln  string
	err error
}

func main() {
	flag.Parse()
	ctx := context.Background()
	debug := false
	addrs := []string{"http://127.0.0.1:8080"}

	cl := client.NewSimple(addrs)
	// 读持久化
	if buf, err := ioutil.ReadFile(fmt.Sprintf(simpleStateFilePath, *categoryName)); err == nil {
		if err := cl.RestoreSavedState(buf); err != nil {
			log.Printf("无法恢复保存的客户端状态: %v", err)
		}
	}
	cl.SetDebug(debug)
	fmt.Printf("在提示中输入消息以将其发送到yerkYar副本之一\n")
	go printContinuously(ctx, cl, debug)

	rd := bufio.NewReader(os.Stdin)
	fmt.Printf("> ")

	sigCh := make(chan os.Signal, 5)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	readCh := make(chan readResult)
	go func() {
		for {
			ln, err := rd.ReadString('\n')
			readCh <- readResult{ln: ln, err: err}
		}
	}()

	for {
		var ln string
		var err error

		select {
		case s := <-sigCh:
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

		if err := cl.Send(ctx, *categoryName, []byte(ln)); err != nil {
			log.Printf("向yerkYar发送数据失败: %v", err)
		}

		fmt.Printf("> ")
	}
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
		err := cl.Process(ctx, *categoryName, scratch, func(b []byte) error {
			fmt.Printf("\n")
			log.Printf("BATCH: %s", b)
			fmt.Printf("> ")
			return nil
		})
		if err != nil {
			fmt.Println(err)
		}

		if debug {
			time.Sleep(time.Millisecond * 10000)
		} else {
			time.Sleep(time.Millisecond * 100)
		}
	}
}
