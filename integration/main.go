package integration

import (
	"context"
	"errors"
	"fmt"
	"github.com/yanyanran/yerfYar/server"
	"github.com/yanyanran/yerfYar/server/replication"
	"github.com/yanyanran/yerfYar/web"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type InitArgs struct {
	EtcdAddr []string

	ClusterName  string
	InstanceName string

	DirName    string
	ListenAddr string
}

type OnDiskCreator struct {
	dirName      string
	instanceName string
	replStorage  *replication.Storage

	m        sync.Mutex
	storages map[string]*server.OnDisk
}

// InitAndServe 检查所提供参数的有效性并在指定端口上启动 Web 服务器 (instanceName-"xx-chunk"->"xx"
func InitAndServe(a InitArgs) error {
	log.SetPrefix("[" + a.InstanceName + "] ")

	replState, err := replication.NewState(a.EtcdAddr, a.ClusterName)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	if err := replState.RegisterNewPeer(ctx, replication.Peer{
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

	replStorage := replication.NewStorage(replState, a.InstanceName)
	creator := &OnDiskCreator{
		dirName:      a.DirName,
		instanceName: a.InstanceName,
		replStorage:  replStorage,
		storages:     make(map[string]*server.OnDisk),
	}

	s := web.NewServer(replState, a.InstanceName, a.DirName, a.ListenAddr, replStorage, creator.Get)

	replClient := replication.NewCompClient(replState, creator, a.InstanceName)
	go replClient.Loop(context.Background())

	log.Printf("Listening connections")
	return s.Serve()
}

func (c *OnDiskCreator) Stat(category string, fileName string) (size int64, exists bool, err error) {
	st, err := os.Stat(filepath.Join(c.dirName, category, fileName))
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	} else if err != nil {
		return 0, false, err
	}
	return st.Size(), true, nil
}

func (c *OnDiskCreator) WriteDirect(category string, fileName string, contents []byte) error {
	inst, err := c.Get(category)
	if err != nil {
		return err
	}
	return inst.WriteDirect(fileName, contents)
}

func (c *OnDiskCreator) Get(category string) (*server.OnDisk, error) {
	c.m.Lock()
	defer c.m.Unlock()

	storage, ok := c.storages[category]
	if ok {
		return storage, nil
	}

	storage, err := c.newOnDisk(category)
	if err != nil {
		return nil, err
	}

	c.storages[category] = storage
	return storage, nil
}

func (c *OnDiskCreator) newOnDisk(category string) (*server.OnDisk, error) {
	dir := filepath.Join(c.dirName, category)
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, fmt.Errorf("为category创建目录: %v", err)
	}
	return server.NewOnDisk(dir, category, c.instanceName, c.replStorage)
}