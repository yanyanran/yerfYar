package server

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestInitLastChunkIdx(t *testing.T) {
	dir := getTempDir(t)

	testCreateFile(t, filepath.Join(dir, "chunk1"))
	testCreateFile(t, filepath.Join(dir, "chunk10"))
	srv := testNewOnDisk(t, dir)

	want := uint64(11)
	got := srv.lastChunkIdx

	if got != want {
		t.Errorf("Last chunk index = %d, want %d", got, want)
	}
}

func TestGetFileDescriptor(t *testing.T) {
	dir := getTempDir(t)
	testCreateFile(t, filepath.Join(dir, "chunk1"))
	srv := testNewOnDisk(t, dir)

	testCases := []struct {
		desc     string
		filename string
		write    bool
		wantErr  bool
	}{
		{
			desc:     "从现有文件读取, 不应失败",
			filename: "chunk1",
			write:    false,
			wantErr:  false,
		},
		{
			desc:     "不应覆盖现有文件",
			filename: "chunk1",
			write:    true,
			wantErr:  true,
		},
		{
			desc:     "不应该从不存在的文件中读取",
			filename: "chunk2",
			write:    false,
			wantErr:  true,
		},
		{
			desc:     "应该能创建不存在的文件",
			filename: "chunk2",
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
	if err := srv.Write([]byte(want)); err != nil {
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

	// Check that the last message is not chopped and only the first
	// three messages are returned if the buffer size is too small to
	// fit all four messages.
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
	if err := srv.Write([]byte(want)); err != nil {
		t.Fatalf("想要没有错误: %v", err)
	}

	chunks, err := srv.ListChunks()
	if err != nil {
		t.Fatalf("ListChunks(): %v", err)
	}

	if got, want := len(chunks), 1; want != got {
		t.Fatalf("len(ListChunks()) = %d, want %d", got, want)
	}

	if err := srv.Ack(chunks[0].Name, chunks[0].Size); err == nil {
		t.Errorf("Ack(last chunk): 没有错误，预计会出现错误")
	}
}

func TestAckOfTheCompleteChunk(t *testing.T) {
	dir := getTempDir(t)
	srv := testNewOnDisk(t, dir)
	testCreateFile(t, filepath.Join(dir, "chunk1"))

	if err := srv.Ack("chunk1", 0); err != nil {
		t.Errorf("Ack(chunk1) = %v, 预计不会出现错误", err)
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

func testNewOnDisk(t *testing.T, dir string) *OnDisk {
	t.Helper()

	srv, err := NewOnDisk(dir)
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
