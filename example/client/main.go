package main

import (
	"bufio"
	"fmt"
	"github.com/yanyanran/yerfYar/client"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

const simpleStateFilePath = "/tmp/simple-example-state.json"

type readResult struct {
	ln  string
	err error
}

func main() {
	addrs := []string{"http://127.0.0.1:8080", "http://127.0.0.1:8081", "http://10.30.0.154:8080"}

	cl := client.NewSimple(addrs)
	if buf, err := ioutil.ReadFile(simpleStateFilePath); err == nil {
		if err := cl.RestoreSavedState(buf); err != nil {
			log.Printf("无法恢复保存的客户端状态: %v", err)
		}
	}
	cl.Debug = true

	fmt.Printf("在提示中输入消息以将其发送到yerkYar副本之一\n")

	go printContinuously(cl)

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
			saveState(cl)
			return
		} else if err != nil {
			log.Fatalf("读取失败: %v", err)
		}

		if !strings.HasSuffix(ln, "\n") {
			log.Fatalf("该行不完整: %q", ln)
		}

		if err := cl.Send("stdin", []byte(ln)); err != nil {
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
		ioutil.WriteFile(simpleStateFilePath, buf, 0666)
	}
	fmt.Println("")
}

func printContinuously(cl *client.Simple) {
	scratch := make([]byte, 1024*1024)

	for {
		cl.Process("stdin", scratch, func(b []byte) error {
			fmt.Printf("\n")
			log.Printf("BATCH: %s", b)
			fmt.Printf("> ")
			return nil
		})

		//if cl.Debug {
		//	time.Sleep(time.Millisecond * 10000)
		//} else {
		//	time.Sleep(time.Millisecond * 100)
		//}
	}
}
