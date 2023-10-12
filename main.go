package main

import (
	"flag"
	"fmt"
	"github.com/yanyanran/yerfYar/server"
	"github.com/yanyanran/yerfYar/web"
	"log"
	"os"
	"path/filepath"
)

var (
	dirname = flag.String("dirname", "", "The directory name where to put all the data")
	inmem   = flag.Bool("inmem", false, "Whether or not use in-memory storage instead of a disk-based one")
	port    = flag.Uint("port", 8080, "Network port to listen on")
)

func main() {
	flag.Parse()
	var backend web.Storage

	if *inmem {
		backend = &server.InMemory{}
	} else {
		if *dirname == "" {
			log.Fatalf("The flag `--dirname` must be provided")
		}

		filename := filepath.Join(*dirname, "write_test")
		fp, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			log.Fatalf("Could not create test file %q: %s", filename, err)
		}
		fp.Close()
		os.Remove(fp.Name()) // 删除write_test文件

		backend = server.NewOnDisk(*dirname)
	}

	s := web.NewServer(backend, *port)
	log.Printf("Listening connections")

	err := s.Serve()
	if err != nil {
		fmt.Printf(err.Error())
	}
}
