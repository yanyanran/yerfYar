package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
)

const systemReplication = "_system-replication"
const systemAck = "_system-ack"

// Storage 为磁盘存储提供hooks，调用以确保复制chunk
type Storage struct {
	logger          *log.Logger
	dw              DirectWriter
	currentInstance string
}

func NewStorage(logger *log.Logger, dw DirectWriter, currentInstance string) *Storage {
	return &Storage{
		logger:          logger,
		dw:              dw,
		currentInstance: currentInstance,
	}
}

func (s *Storage) AfterCreatingChunk(ctx context.Context, category string, fileName string) error {
	if err := s.dw.SetReplicationDisabled(systemReplication, true); err != nil {
		return fmt.Errorf("SetReplicationDisabled发生错误: %v", err)
	}

	ch := Chunk{
		Owner:    s.currentInstance,
		Category: category,
		FileName: fileName,
	}

	buf, err := json.Marshal(&ch)
	if err != nil {
		return fmt.Errorf("marshalling chunk: %v", err)
	}
	buf = append(buf, '\n')

	if _, _, err := s.dw.Write(ctx, systemReplication, buf); err != nil {
		return fmt.Errorf("将chunk写入 systemReplication 类别: %v", err)
	}
	return nil
}

func (s *Storage) AfterAckChunk(ctx context.Context, category string, fileName string) error {
	if err := s.dw.SetReplicationDisabled(systemAck, true); err != nil {
		return fmt.Errorf("setting replication disabled: %v", err)
	}

	ch := Chunk{
		Owner:    s.currentInstance,
		Category: category,
		FileName: fileName,
	}

	buf, err := json.Marshal(&ch)
	if err != nil {
		return fmt.Errorf("marshalling chunk发生错误: %v", err)
	}
	buf = append(buf, '\n')

	if _, _, err := s.dw.Write(ctx, systemAck, buf); err != nil {
		return fmt.Errorf("将chunk写入系统 systemAck 类别: %v", err)
	}
	return nil
}
