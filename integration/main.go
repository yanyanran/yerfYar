package integration

import (
	"context"
	"fmt"
	"github.com/yanyanran/yerfYar/server/replication"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yanyanran/yerfYar/web"
	"go.etcd.io/etcd/clientv3"
)

// InitAndServe 检查所提供参数的有效性并在指定端口上启动 Web 服务器 (instanceName-"xx-chunk"->"xx"
func InitAndServe(etcdAddr string, instanceName string, dirname string, listenAddr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(etcdAddr, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("创建新客户: %w", err)
	}
	defer etcdClient.Close()

	_, err = etcdClient.Put(ctx, "test", "test")
	if err != nil {
		return fmt.Errorf("无法设置测试key: %w", err)
	}

	_, err = etcdClient.Put(ctx, "peers/"+instanceName, listenAddr)
	if err != nil {
		return fmt.Errorf("无法在 etcd 中注册 peer 地址: %w", err)
	}

	filename := filepath.Join(dirname, "write_test")
	fp, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("creating test file %q: %s", filename, err)
	}
	fp.Close()
	os.Remove(fp.Name())

	s := web.NewServer(etcdClient, instanceName, dirname, listenAddr, replication.NewStorage(etcdClient, instanceName))

	log.Printf("Listening connections")
	return s.Serve()
}
