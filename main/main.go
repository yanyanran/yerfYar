package main

import (
	"fmt"
	"github.com/yanyanran/yerfYar/client"
)

// 小测试
func main() {
	addr := make([]string, 0)
	addr = append(addr, "http://127.0.0.1:8080")
	client := client.NewSimple(addr)

	msg := []byte("yes!\n")
	err := client.Send("hah", msg)
	if err != nil {
		println(err.Error())
	}

	rev := make([]byte, 10)
	client.Process("hah", rev, func(rev []byte) error {
		fmt.Println(rev)
		return nil
	})

	client.Process("hah", rev, func(rev []byte) error {
		fmt.Println(rev)
		return nil
	})

	println(string(rev))
}
