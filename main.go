package main

import (
	"fmt"
	"log"
	"yerfYar/server"
	"yerfYar/web"
)

func main() {
	s := web.NewServer(&server.InMemory{})
	log.Printf("Listening connections")

	err := s.Serve()
	if err != nil {
		fmt.Printf(err.Error())
	}
}
