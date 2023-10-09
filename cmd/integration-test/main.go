package main

import (
	"log"
	"yerfYar/server"
	"yerfYar/web"
)

func main() {
	s := web.NewServer(&server.InMemory{})

	log.Printf("Listening connections")
	s.Serve()
}
