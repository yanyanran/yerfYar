package client

import (
	"github.com/yanyanran/yerfYar/protocol"
	"reflect"
	"testing"
)

func TestSimpleMarshalUnmarshal(t *testing.T) {
	st := &state{
		Offsets: map[string]ReadOffset{
			"Moscow": {
				CurChunk: protocol.Chunk{
					Name:     "Moscow-test000001",
					Complete: true,
					Size:     123456,
				},
				Offset: 123,
			},
			"hah": {
				CurChunk: protocol.Chunk{
					Name:     "hah-test000001",
					Complete: true,
					Size:     345678,
				},
				Offset: 345,
			},
		},
	}

	cl := NewSimple([]string{"http://localhost"})
	cl.st = st

	buf, err := cl.MarshalState()
	if err != nil {
		t.Errorf("MarshalState() = ..., %v", err)
	}

	if l := len(buf); l <= 2 {
		t.Errorf("MarshalState() = 长度为 %d 的字节片，...；希望长度至少为3", l)
	}

	cl = NewSimple([]string{"http://localhost"})
	err = cl.RestoreSavedState(buf)

	if err != nil {
		t.Errorf("RestoreSavedState(...) = %v", err)
	}

	if !reflect.DeepEqual(st, cl.st) {
		t.Errorf("RestoreSavedState() 恢复 %+v; 想要 %+v", cl.st, st)
	}
}
