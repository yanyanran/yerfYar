package integration

import (
	"context"
	"fmt"
	"github.com/yanyanran/yerfYar/server/replication"
	"github.com/yanyanran/yerfYar/web"
	"log"
	"os"
	"path/filepath"
	"time"
)

type InitArgs struct {
	EtcdAddr []string

	ClusterName  string
	InstanceName string

	DirName    string
	ListenAddr string
}

// InitAndServe 检查所提供参数的有效性并在指定端口上启动 Web 服务器 (instanceName-"xx-chunk"->"xx"
func InitAndServe(a InitArgs) error {
	client, err := replication.NewClient(a.EtcdAddr, a.ClusterName)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	if err := client.RegisterNewPeer(ctx, replication.Peer{
		InstanceName: a.InstanceName,
		ListenAddr:   a.ListenAddr,
	}); err != nil {
		return fmt.Errorf("无法在 etcd 中注册 peer 地址: %w", err)
	}

	filename := filepath.Join(a.DirName, "write_test")
	fp, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("creating test file %q: %s", filename, err)
	}
	fp.Close()
	os.Remove(fp.Name())

	repl := replication.NewStorage(client, a.InstanceName)
	s := web.NewServer(client, a.InstanceName, a.DirName, a.ListenAddr, repl)

	log.Printf("Listening connections")
	return s.Serve()
}
