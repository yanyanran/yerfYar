package main

import (
	"flag"
	"fmt"
	"log"
	"yerfYar/web"
)

var (
	filename = flag.String("filename", "", "The filename where to put all the data")
	inmem    = flag.Bool("inmem", false, "Whether or not use in-memory storage instead of a disk-based one")
	port     = flag.Uint("port", 8080, "Network port to listen on")
)

func main() {
	flag.Parse()
	var backend web.Storage

	//if *inmem {
	//	backend = &server.InMemory{}
	//} else {
	//	if *filename == "" {
	//		log.Fatalf("The flag `--filename` must be provided")
	//	}
	//
	//	fp, err := os.OpenFile(*filename, os.O_CREATE|os.O_RDWR, 0666)
	//	if err != nil {
	//		log.Fatalf("Could not create file %q: %s", *filename, err)
	//	}
	//	defer fp.Close()
	//
	//	backend = server.NewOnDisk(fp)
	//}

	s := web.NewServer(backend, *port)
	log.Printf("Listening connections")

	err := s.Serve()
	if err != nil {
		fmt.Printf(err.Error())
	}
}
