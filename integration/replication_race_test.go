package integration

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/yanyanran/yerfYar/client"
)

func ensureChunkDoesNotExist(t *testing.T, cl *client.Raw, addr string, chunk string, errMsg string) {
	chunks, err := cl.ListChunks(context.Background(), addr, "race", false)
	if err != nil {
		t.Fatalf("%q 处的 ListChunks() 返回错误：%v", addr, err)
	}

	for _, ch := range chunks {
		if ch.Name == chunk {
			t.Fatalf("%q 处有意外chunk %q，它不应该存在 (%s)", chunk, addr, errMsg)
		}
	}
}

func ensureChunkExists(t *testing.T, cl *client.Raw, addr string, chunk string, errMsg string) {
	chunks, err := cl.ListChunks(context.Background(), addr, "race", false)
	if err != nil {
		t.Fatalf("%q 处的 ListChunks() 返回错误：%v", addr, err)
	}

	for _, ch := range chunks {
		if ch.Name == chunk {
			return
		}
	}

	t.Fatalf("在 %q 处未找到chunk %q，希望它存在", chunk, addr)
}

func waitUntilChunkAppears(t *testing.T, cl *client.Raw, addr string, chunk string, errMsg string) {
	t.Helper()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*10))
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("等待chunk %q 出现在 %q (%s) 时超时", chunk, addr, errMsg)
		default:
		}

		chunks, err := cl.ListChunks(ctx, addr, "race", false)
		if err != nil {
			t.Fatalf("%q 处的 ListChunks() 返回错误：%v", addr, err)
		}

		for _, ch := range chunks {
			if ch.Name == chunk {
				return
			}
		}

		time.Sleep(time.Millisecond * 100)
	}
}

func moscowChunkName(idx int) string {
	return fmt.Sprintf("moscow-chunk%09d", idx)
}

func mustSend(t *testing.T, cl *client.Simple, ctx context.Context, category string, msgs []byte) {
	err := cl.Send(ctx, category, msgs)
	if err != nil {
		t.Fatalf("无法发送以下消息：%q，错误：%v", string(msgs), err)
	}
}

func TestReplicatingAlreadyAcknowledgedChunk(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	addrs, _ := runChukcha(t, true, tweaks{
		modifyInitArgs: func(t *testing.T, a *InitArgs) {
			a.DisableAck = true
			a.MaxChunkSize = 10
		},
	})

	rawClient := client.NewRaw(&http.Client{})
	moscowClient := client.NewSimple(addrs[0:1])
	voronezhClient := client.NewSimple(addrs[1:2])
	// voronezhClient.Debug = true

	firstMsg := "Moscow is having chunk0 which starts to replicate to Voronezh immediately\n"

	// 在Moscow创建 chunk0
	mustSend(t, moscowClient, ctx, "race", []byte(firstMsg))
	ensureChunkExists(t, rawClient, addrs[0], moscowChunkName(0), "第一次发送后 Chunk0 必须存在")

	// 在Moscow创建 chunk1
	mustSend(t, moscowClient, ctx, "race", []byte("Moscow now has chunk1 that is being written into currently\n"))
	ensureChunkExists(t, rawClient, addrs[0], moscowChunkName(1), "Chunk1 must be present after second send")

	waitUntilChunkAppears(t, rawClient, addrs[1], moscowChunkName(1), "Voronezh must have chunk1 via replication")

	// 阅读Moscow的 chunk0 直到最后并确认它
	err := voronezhClient.Process(ctx, "race", nil, func(msg []byte) error {
		if !bytes.Equal(msg, []byte(firstMsg)) {
			t.Fatalf("读取意外消息：%q 而不是 %q", string(msg), firstMsg)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("处理Voronezh第一个chunk期间预计不会出现错误: %v", err)
	}

	ensureChunkExists(t, rawClient, addrs[1], moscowChunkName(0), "Chunk0 must not have been acknowledged when reading from Voronezh the first time")

	// TODO: 要在简单客户端中触发确认，我们需要再次读取相同的块，以便它看到该块是完整的并且可以被确认。这不应该是必需的
	err = voronezhClient.Process(ctx, "race", nil, func(msg []byte) error {
		return nil
	})
	if err != nil {
		t.Fatalf("No errors expected during processing of the second chunk in Voronezh: %v", err)
	}

	ensureChunkDoesNotExist(t, rawClient, addrs[1], moscowChunkName(0), "Chunk0 must have been acknowledged when reading from Voronezh")

	// 在Moscow创建 chunk2
	mustSend(t, moscowClient, ctx, "race", []byte("Moscow now has chunk2 that is being written into currently, and this will also create an entry to replication which will try to download chunk0 on Voronezh once again, even though it was acknowledged on Voronezh, and, as acknowledge thread is disabled in this test, it will mean that Voronezh will download chunk0 once again\n"))
	ensureChunkExists(t, rawClient, addrs[0], moscowChunkName(2), "Chunk2 must be present after third send()")

	// 确保 chunk2 已开始下载
	waitUntilChunkAppears(t, rawClient, addrs[1], moscowChunkName(2), "Chunk2 must be present on Voronezh via replication")

	// 如果一切正常，这意味着Moscow chunk0 将不会下载到Voronezh，因为它已经在那里得到了ack
	ensureChunkDoesNotExist(t, rawClient, addrs[1], moscowChunkName(0), "Chunk0 must not be present on Voronezh because it was previously acknowledged")
}
