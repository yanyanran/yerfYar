package replication

import (
	"context"
	"fmt"
)

// Storage 为磁盘存储提供hooks，调用以确保复制chunk
type Storage struct {
	client          *Client
	currentInstance string
}

func NewStorage(client *Client, currentInstance string) *Storage {
	return &Storage{
		client:          client,
		currentInstance: currentInstance,
	}
}

func (s *Storage) BeforeCreatingChunk(ctx context.Context, category string, fileName string) error {
	peers, err := s.client.ListPeers(ctx)
	if err != nil {
		return fmt.Errorf("从 etcd 获取 peer: %v", err)
	}

	for _, p := range peers {
		if p.InstanceName == s.currentInstance {
			continue
		}

		if err := s.client.AddChunkToReplicationQueue(ctx, p.InstanceName, Chunk{
			Owner:    s.currentInstance,
			Category: category,
			FileName: fileName,
		}); err != nil {
			return fmt.Errorf("无法写入%q（%q）的复制队列：%w", p.InstanceName, p.ListenAddr, err)
		}
	}

	return nil
}
