package integration

import (
	"context"
	"errors"
	"fmt"
	"github.com/yanyanran/yerfYar/server"
	"github.com/yanyanran/yerfYar/server/replication"
	"github.com/yanyanran/yerfYar/web"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type InitArgs struct {
	LogWriter io.Writer
	Peers     []replication.Peer

	ClusterName  string
	InstanceName string

	DirName    string
	ListenAddr string

	MaxChunkSize        uint64
	RotateChunkInterval time.Duration
	DisableAck          bool
}

type OnDiskCreator struct {
	logger              *log.Logger
	dirName             string
	instanceName        string
	replStorage         *replication.Storage
	maxChunkSize        uint64
	rotateChunkInterval time.Duration

	m        sync.Mutex
	storages map[string]*server.OnDisk
}

// InitAndServe 检查所提供参数的有效性并在指定端口上启动 Web 服务器 (instanceName-"xx-chunk"->"xx"
func InitAndServe(a InitArgs) error {
	logger := log.New(a.LogWriter, "["+a.InstanceName+"] ", log.LstdFlags|log.Lmicroseconds)

	filename := filepath.Join(a.DirName, "write_test")
	fp, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("creating test file %q: %s", filename, err)
	}
	fp.Close()
	os.Remove(fp.Name())

	creator := &OnDiskCreator{
		logger:              logger,
		dirName:             a.DirName,
		instanceName:        a.InstanceName,
		storages:            make(map[string]*server.OnDisk),
		maxChunkSize:        a.MaxChunkSize,
		rotateChunkInterval: a.RotateChunkInterval,
	}
	replStorage := replication.NewStorage(logger, creator, a.InstanceName)
	creator.replStorage = replStorage

	s := web.NewServer(logger, a.InstanceName, a.DirName, a.ListenAddr, replStorage, creator.Get)

	replClient := replication.NewCompClient(logger, a.DirName, creator, a.Peers, a.InstanceName)
	go replClient.Loop(context.Background(), a.DisableAck)

	logger.Printf("Listening connections at %q", a.ListenAddr)
	return s.Serve()
}

func (c *OnDiskCreator) Stat(category string, fileName string) (size int64, exists bool, delete bool, err error) {
	filePath := filepath.Join(c.dirName, category, fileName)

	st, err := os.Stat(filePath)
	if errors.Is(err, os.ErrNotExist) {
		_, deletedErr := os.Stat(filePath + server.DeletedSuffix)
		if errors.Is(deletedErr, os.ErrNotExist) {
			return 0, false, false, nil
		} else if deletedErr != nil {
			return 0, false, false, deletedErr
		}

		return 0, false, true, nil
	} else if err != nil {
		return 0, false, false, err
	}
	return st.Size(), true, false, nil
}

func (c *OnDiskCreator) WriteDirect(category string, fileName string, contents []byte) error {
	inst, err := c.Get(category)
	if err != nil {
		return err
	}
	return inst.WriteDirect(fileName, contents)
}

func (c *OnDiskCreator) SetReplicationDisabled(category string, v bool) error {
	inst, err := c.Get(category)
	if err != nil {
		return err
	}
	inst.SetReplicationDisabled(v)
	return nil
}

func (c *OnDiskCreator) Write(ctx context.Context, category string, msgs []byte) (chunkName string, off int64, err error) {
	inst, err := c.Get(category)
	if err != nil {
		return "", 0, err
	}
	return inst.Write(ctx, msgs)
}

func (c *OnDiskCreator) AckDirect(ctx context.Context, category string, chunk string) error {
	inst, err := c.Get(category)
	if err != nil {
		return err
	}

	return inst.AckDirect(chunk)
}

func (c *OnDiskCreator) Get(category string) (*server.OnDisk, error) {
	c.m.Lock()
	defer c.m.Unlock()

	storage, ok := c.storages[category]
	if ok {
		return storage, nil
	}

	storage, err := c.newOnDisk(c.logger, category)
	if err != nil {
		return nil, err
	}

	c.storages[category] = storage
	return storage, nil
}

func (c *OnDiskCreator) newOnDisk(logger *log.Logger, category string) (*server.OnDisk, error) {
	dir := filepath.Join(c.dirName, category)
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, fmt.Errorf("为category创建目录: %v", err)
	}
	return server.NewOnDisk(logger, dir, category, c.instanceName, c.maxChunkSize, c.rotateChunkInterval, c.replStorage)
}
