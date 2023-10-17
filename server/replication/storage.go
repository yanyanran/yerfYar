package replication

import (
	"context"
	"fmt"
	"strings"

	"go.etcd.io/etcd/clientv3"
)

// Storage 为磁盘存储提供hooks，调用以确保复制chunk
type Storage struct {
	client          *clientv3.Client
	currentInstance string
}

func NewStorage(client *clientv3.Client, currentInstance string) *Storage {
	return &Storage{
		client:          client,
		currentInstance: currentInstance,
	}
}

func (s *Storage) BeforeCreatingChunk(ctx context.Context, category string, fileName string) error {
	resp, err := s.client.Get(ctx, "peers/", clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("从 etcd 获取 peer: %v", err)
	}

	for _, kv := range resp.Kvs {
		key := strings.TrimPrefix(string(kv.Key), "peers/") // 去除前缀peers
		if key == s.currentInstance {
			continue
		}

		_, err = s.client.Put(ctx, "replication/"+key+"/"+category+"/"+fileName, s.currentInstance) // etcd存储kv：chunk-server
		if err != nil {
			return fmt.Errorf("无法写入 %q (%q) 的复制队列：%w", key, string(kv.Value), err)
		}
	}

	return nil
}
