package protocol

// Chunk 一段数据，包含写入其中的消息
type Chunk struct {
	Name     string `json:"name"`
	Complete bool   `json:"complete"` // false:不完整意味着正在被写入
	Size     uint64 `json:"size"`
}
