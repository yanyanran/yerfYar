package heap

import "container/heap"

//type Ordered interface {
//	Less(a int) bool // 元素值小于v返回true
//}
//
//type underlying []int
//
//type Min struct {
//	h *underlying
//}
//
//func NewMin() Min {
//	h := &underlying{}
//	heap.Init(h)
//	return Min{h: h}
//}

type Ordered[T any] interface {
	Less(v T) bool // 元素值小于v返回true
}

type underlying[T Ordered[T]] []T

type Min[T Ordered[T]] struct {
	h *underlying[T]
}

func NewMin[T Ordered[T]]() Min[T] {
	h := &underlying[T]{}
	heap.Init(h)
	return Min[T]{h: h}
}

func (h *Min[T]) Push(x T) {
	heap.Push(h.h, x)
}

func (h *Min[T]) Pop() T {
	return heap.Pop(h.h).(T)
}

func (h *Min[T]) Len() int {
	return len(*h.h)
}

func (h underlying[T]) Len() int           { return len(h) }
func (h underlying[T]) Less(i, j int) bool { return h[i].Less(h[j]) }
func (h underlying[T]) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

// Push和Pop使用指针接收器，因为它们修改切片长度而不仅仅是其内容

func (h *underlying[T]) Push(x any) {

	*h = append(*h, x.(T))
}

func (h *underlying[T]) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
