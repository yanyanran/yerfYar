package replication

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.etcd.io/etcd/clientv3"
)

const defaultTimeout = 10 * time.Second

// Client 用于存储有关复制状态信息的持久kv存储的包装器
type Client struct {
	cl     *clientv3.Client
	prefix string // 词头
}

type Result struct {
	Key   string
	Value string
}

type Peer struct {
	InstanceName string
	ListenAddr   string
}

type Chunk struct {
	Owner    string
	Category string
	FileName string
}

type Option clientv3.OpOption

// NewClient 初始化与etcd集群的连接
func NewClient(addr []string, clusterName string) (*Client, error) {
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   addr,
		DialTimeout: defaultTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("创建etcd客户端: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	_, err = etcdClient.Put(ctx, "test", "test")
	if err != nil {
		return nil, fmt.Errorf("无法设置测试key: %w", err)
	}

	return &Client{
		cl:     etcdClient,
		prefix: "yerkYar/" + clusterName + "/",
	}, nil
}

func (c *Client) put(ctx context.Context, key, value string) error {
	_, err := c.cl.Put(ctx, c.prefix+key, value)
	return err
}

func (c *Client) get(ctx context.Context, key string, opts ...Option) ([]Result, error) {
	etcdOpts := make([]clientv3.OpOption, 0, len(opts))
	for _, o := range opts {
		etcdOpts = append(etcdOpts, clientv3.OpOption(o))
	}

	etcdRes, err := c.cl.Get(ctx, c.prefix+key, etcdOpts...)
	if err != nil {
		return nil, err
	}

	res := make([]Result, 0, len(etcdRes.Kvs))
	for _, kv := range etcdRes.Kvs {
		res = append(res, Result{
			Key:   string(kv.Key),
			Value: string(kv.Value),
		})
	}

	return res, nil
}

func (c *Client) RegisterNewPeer(ctx context.Context, p Peer) error {
	return c.put(ctx, "peers/"+p.InstanceName, p.ListenAddr)
}

func WithPrefix() Option {
	return Option(clientv3.WithPrefix())
}

func (c *Client) ListPeers(ctx context.Context) ([]Peer, error) {
	resp, err := c.get(ctx, "peers/", WithPrefix())
	if err != nil {
		return nil, err
	}

	res := make([]Peer, 0, len(resp))
	for _, kv := range resp {
		res = append(res, Peer{
			InstanceName: strings.TrimPrefix(kv.Key, c.prefix+"peers/"),
			ListenAddr:   kv.Value,
		})
	}

	return res, nil
}

func (c *Client) AddChunkToReplicationQueue(ctx context.Context, targetInstance string, ch Chunk) error {
	key := "replication/" + targetInstance + "/" + ch.Category + "/" + ch.FileName
	return c.put(ctx, key, ch.Owner)
}
