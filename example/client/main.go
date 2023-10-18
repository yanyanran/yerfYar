package main

import (
	"bufio"
	"fmt"
	"github.com/yanyanran/yerfYar/client"
	"log"
	"os"
	"strings"
	"time"
)

func main() {
	addrs := []string{"http://127.0.0.1:8080", "http://127.0.0.1:8081"}

	cl := client.NewSimple(addrs)
	// cl.Debug = true

	fmt.Printf("在提示中输入消息以将其发送到yerkYar副本之一\n")

	go printContinuously(cl)

	rd := bufio.NewReader(os.Stdin)
	fmt.Printf("> ")

	for {
		ln, err := rd.ReadString('\n')
		if err != nil {
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

func printContinuously(cl *client.Simple) {
	scratch := make([]byte, 1024*1024)

	for {
		cl.Process("stdin", scratch, func(b []byte) error {
			fmt.Printf("\n")
			log.Printf("BATCH: %s", b)
			fmt.Printf("> ")
			return nil
		})

		time.Sleep(time.Millisecond * 10000)
	}

}
