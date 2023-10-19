package protocol

import (
	"regexp"
	"strconv"
	"strings"
)

var filenameRegexp = regexp.MustCompile("^chunk([0-9]+)$") // 匹配以"chunk"开头，后跟一个或多个数字的字符串

// Chunk 一段数据，包含写入其中的消息
type Chunk struct {
	Name     string `json:"name"`
	Complete bool   `json:"complete"` // false:不完整意味着正在被写入
	Size     uint64 `json:"size"`
}

func ParseChunkFileName(filename string) (instance string, chunkIdx int) {
	var err error

	idx := strings.LastIndexByte(filename, '-')
	if idx < 0 {
		return "", 0
	}

	instance = filename[0:idx]    // “hah-chunk123”-> "hah"
	chunkName := filename[idx+1:] // “hah-chunk123”-> "chunk123"

	res := filenameRegexp.FindStringSubmatch(chunkName)
	if res == nil {
		return "", 0
	}

	chunkIdx, err = strconv.Atoi(res[1]) // “hah-chunk123”-> "123"
	if err != nil {
		return "", 0
	}

	return instance, chunkIdx
}
