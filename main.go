package main

import (
	"flag"
	"github.com/yanyanran/yerfYar/integration"
	"log"
	"os"
	"strings"
)

var (
	clusterName  = flag.String("cluster", "default", "集群名称（必须指定是否与多个yerkYar实例共享单个etcd实例）")
	instanceName = flag.String("instance", "", "唯一的实例名称（例如 yerkYar1）")
	dirname      = flag.String("dirname", "", "The directory name where to put all the data")
	listenAddr   = flag.String("listen", "127.0.0.1:8080", "Network address to listen on")
	etcdAddr     = flag.String("etcd", "http://127.0.0.1:2379", "The network address of etcd server(s)")
)

func main() {
	flag.Parse()

	if *clusterName == "" {
		log.Fatalf("The flag `--cluster` must not be empty")
	}

	if *instanceName == "" {
		log.Fatalf("The flag `--instance` must be provided")
	}

	if *dirname == "" {
		log.Fatalf("The flag `--dirname` must be provided")
	}

	if *etcdAddr == "" {
		log.Fatalf("The flag `--etcd` must be provided")
	}

	a := integration.InitArgs{
		LogWriter:    os.Stderr,
		EtcdAddr:     strings.Split(*etcdAddr, ","),
		ClusterName:  *clusterName,
		InstanceName: *instanceName,
		DirName:      *dirname,
		ListenAddr:   *listenAddr,
		MaxChunkSize: 20 * 1024 * 1024,
	}

	if err := integration.InitAndServe(a); err != nil {
		log.Fatalf("InitAndServe failed: %v", err)
	}
}
