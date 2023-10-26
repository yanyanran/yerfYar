package main

import (
	"flag"
	"github.com/yanyanran/yerfYar/integration"
	"log"
	"os"
	"strings"
	"time"
)

var (
	clusterName         = flag.String("cluster", "default", "集群名称（必须指定是否与多个yerkYar实例共享单个etcd实例）")
	instanceName        = flag.String("instance", "", "唯一的实例名称（例如 yerfYar1）")
	dirname             = flag.String("dirname", "", "The directory name where to put all the data")
	listenAddr          = flag.String("listen", "127.0.0.1:8080", "Network address to listen on")
	etcdAddr            = flag.String("etcd", "http://127.0.0.1:2379", "The network address of etcd server(s)")
	maxChunkSize        = flag.Uint64("max-chunk-size", 20*1024*1024, "chunk最大大小")
	rotateChunkInterval = flag.Duration("rotate-chunk-interval", 10*time.Minute, "无论是否达到最大chunk大小，创建新chunk的频率（有助于回收空间）")
)

func main() {
	flag.Parse()

	if *clusterName == "" {
		log.Fatalf("标志 “--cluster” 不得为空")
	}

	if *instanceName == "" {
		log.Fatalf("必须提供 “--instance”")
	}

	if *dirname == "" {
		log.Fatalf("必须提供 “--dirname”")
	}

	if *etcdAddr == "" {
		log.Fatalf("必须提供 “--etcd”")
	}

	a := integration.InitArgs{
		LogWriter:           os.Stderr,
		EtcdAddr:            strings.Split(*etcdAddr, ","),
		ClusterName:         *clusterName,
		InstanceName:        *instanceName,
		DirName:             *dirname,
		ListenAddr:          *listenAddr,
		MaxChunkSize:        *maxChunkSize,
		RotateChunkInterval: *rotateChunkInterval,
	}

	if err := integration.InitAndServe(a); err != nil {
		log.Fatalf("InitAndServe failed: %v", err)
	}
}
