package server

import (
	"bytes"
	"context"
	"github.com/yanyanran/yerfYar/protocol"
	"os"
	"path/filepath"
	"testing"
)

func TestInitLastChunkIdx(t *testing.T) {
	dir := getTempDir(t)

	testCreateFile(t, filepath.Join(dir, "moscow-chunk1"))
	testCreateFile(t, filepath.Join(dir, "moscow-chunk10"))
	srv := testNewOnDisk(t, dir)

	want := uint64(11)
	got := srv.lastChunkIdx

	if got != want {
		t.Errorf("Last chunk index = %d, want %d", got, want)
	}
}

func TestGetFileDescriptor(t *testing.T) {
	dir := getTempDir(t)
	testCreateFile(t, filepath.Join(dir, "moscow-chunk1"))
	srv := testNewOnDisk(t, dir)

	testCases := []struct {
		desc     string
		filename string
		write    bool
		wantErr  bool
	}{
		{
			desc:     "从现有文件读取, 不应失败",
			filename: "moscow-chunk1",
			write:    false,
			wantErr:  false,
		},
		{
			desc:     "不应覆盖现有文件",
			filename: "moscow-chunk1",
			write:    true,
			wantErr:  true,
		},
		{
			desc:     "不应该从不存在的文件中读取",
			filename: "moscow-chunk2",
			write:    false,
			wantErr:  true,
		},
		{
			desc:     "应该能创建不存在的文件",
			filename: "moscow-chunk2",
			write:    true,
			wantErr:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := srv.getFileDescriptor(tc.filename, tc.write)
			defer srv.forgetFileDescriptor(tc.filename)

			if tc.wantErr && err == nil {
				t.Errorf("wanted error, got not errors")
			} else if !tc.wantErr && err != nil {
				t.Errorf("wanted no errors, got error %v", err)
			}
		})
	}
}

func TestReadWrite(t *testing.T) {
	srv := testNewOnDisk(t, getTempDir(t))

	want := "one\ntwo\nthree\nfour\n"
	if err := srv.Write(context.Background(), []byte(want)); err != nil {
		t.Fatalf("写入失败: %v", err)
	}

	chunks, err := srv.ListChunks()
	if err != nil {
		t.Fatalf("ListChunks(): %v", err)
	}

	if got, want := len(chunks), 1; want != got {
		t.Fatalf("len(ListChunks()) = %d, want %d", got, want)
	}

	chunk := chunks[0].Name

	var b bytes.Buffer
	if err := srv.Read(chunk, 0, uint64(len(want)), &b); err != nil {
		t.Fatalf("Read(%q, 0, %d) = %v, want no errors", chunk, uint64(len(want)), err)
	}

	got := b.String()
	if got != want {
		t.Errorf("Read(%q) = %q, want %q", chunk, got, want)
	}

	// 检查最后一条消息是否未被截断，如果缓冲区大小太小而无法容纳所有四个消息，则仅返回前三个消息。
	want = "one\ntwo\nthree\n"
	b.Reset()
	if err := srv.Read(chunk, 0, uint64(len(want)+1), &b); err != nil {
		t.Fatalf("Read(%q, 0, %d) = %v, want no errors", chunk, uint64(len(want)+1), err)
	}

	got = b.String()
	if got != want {
		t.Errorf("Read(%q) = %q, want %q", chunk, got, want)
	}
}

func TestAckOfTheLastChunk(t *testing.T) {
	srv := testNewOnDisk(t, getTempDir(t))

	want := "one\ntwo\nthree\nfour\n"
	if err := srv.Write(context.Background(), []byte(want)); err != nil {
		t.Fatalf("想要没有错误: %v", err)
	}

	chunks, err := srv.ListChunks()
	if err != nil {
		t.Fatalf("ListChunks(): %v", err)
	}

	if got, want := len(chunks), 1; want != got {
		t.Fatalf("len(ListChunks()) = %d, want %d", got, want)
	}

	if err := srv.Ack(context.Background(), chunks[0].Name, chunks[0].Size); err == nil {
		t.Errorf("Ack(last chunk): 没有错误，预计会出现错误")
	}
}

func TestAckOfTheCompleteChunk(t *testing.T) {
	dir := getTempDir(t)
	srv := testNewOnDisk(t, dir)
	testCreateFile(t, filepath.Join(dir, "moscow-chunk1"))

	if err := srv.Ack(context.Background(), "moscow-chunk1", 0); err != nil {
		t.Errorf("Ack(moscow-chunk1) = %v, 预计不会出现错误", err)
	}
}

func getTempDir(t *testing.T) string {
	t.Helper()

	dir, err := os.MkdirTemp(os.TempDir(), "lastchunkidx")
	if err != nil {
		t.Fatalf("mkdir temp 失败: %v", err)
	}

	t.Cleanup(func() { os.RemoveAll(dir) })

	return dir
}

type nilHooks struct{}

func (n *nilHooks) BeforeCreatingChunk(ctx context.Context, category string, fileName string) error {
	return nil
}

func (n *nilHooks) BeforeAckChunk(ctx context.Context, category string, fileName string) error {
	return nil
}

func testNewOnDisk(t *testing.T, dir string) *OnDisk {
	t.Helper()

	srv, err := NewOnDisk(dir, "test", "moscow", &nilHooks{})
	if err != nil {
		t.Fatalf("NewOnDisk(): %v", err)
	}

	return srv
}

func testCreateFile(t *testing.T, filename string) {
	t.Helper()

	if _, err := os.Create(filename); err != nil {
		t.Fatalf("无法创建文件 %q: %v", filename, err)
	}
}

func TestParseChunkFileName(t *testing.T) {
	testCases := []struct {
		filename     string
		instanceName string
		chunkIdx     int
	}{
		{
			filename:     "Moscow-chunk0000000",
			instanceName: "Moscow",
			chunkIdx:     0,
		},
		{
			filename:     "luna-70-chunk00000123",
			instanceName: "luna-70",
			chunkIdx:     123,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.filename, func(t *testing.T) {
			instance, chunkIdx := protocol.ParseChunkFileName(tc.filename)

			if instance != tc.instanceName || chunkIdx != tc.chunkIdx {
				t.Errorf("parseChunkFileName(%q) = %q, %v; want %q, %v", tc.filename, instance, chunkIdx, tc.instanceName, tc.chunkIdx)
			}
		})
	}
}
