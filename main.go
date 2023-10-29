package main

import (
	"flag"
	"github.com/yanyanran/yerfYar/integration"
	"github.com/yanyanran/yerfYar/server/replication"
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
	peers               = flag.String("peers", "", `集群中其他节点的逗号分隔列表（可以包括自身），例如“Moscow=127.0.0.1：8080，Voronezh=127.0.0.1：8081"`)
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

	a := integration.InitArgs{
		LogWriter:           os.Stderr,
		ClusterName:         *clusterName,
		InstanceName:        *instanceName,
		DirName:             *dirname,
		ListenAddr:          *listenAddr,
		MaxChunkSize:        *maxChunkSize,
		RotateChunkInterval: *rotateChunkInterval,
	}

	if *peers != "" {
		for _, p := range strings.Split(*peers, ",") {
			instanceName, listenAddr, found := strings.Cut(p, "=")
			if !found {
				log.Fatalf("解析标志“--peers”时出错：%q 的对等定义必须采用“<instance_name>=<listenAddr>”格式", p)
			}
			a.Peers = append(a.Peers, replication.Peer{
				InstanceName: instanceName,
				ListenAddr:   listenAddr,
			})
		}
	}

	if err := integration.InitAndServe(a); err != nil {
		log.Fatalf("InitAndServe failed: %v", err)
	}
}
