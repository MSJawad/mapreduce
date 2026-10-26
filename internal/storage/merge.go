package storage

import "container/heap"

// MergeGroups merges record streams that are each sorted by key and calls fn
// once per distinct key, in ascending key order, with every value for that key
// across all streams. Only one key's values are held in memory at a time.
func MergeGroups(readers []*RecordReader, fn func(key string, values []string) error) error {
	h := make(mergeHeap, 0, len(readers))
	for _, rr := range readers {
		if kv, ok := rr.Next(); ok {
			h = append(h, mergeItem{kv: kv, src: rr})
		} else if err := rr.Err(); err != nil {
			return err
		}
	}
	heap.Init(&h)

	var values []string
	for h.Len() > 0 {
		key := h[0].kv.Key
		values = values[:0]
		for h.Len() > 0 && h[0].kv.Key == key {
			top := &h[0]
			values = append(values, top.kv.Value)
			if kv, ok := top.src.Next(); ok {
				top.kv = kv
				heap.Fix(&h, 0)
			} else {
				if err := top.src.Err(); err != nil {
					return err
				}
				heap.Pop(&h)
			}
		}
		if err := fn(key, values); err != nil {
			return err
		}
	}
	return nil
}

type mergeItem struct {
	kv  KeyValue
	src *RecordReader
}

type mergeHeap []mergeItem

func (h mergeHeap) Len() int           { return len(h) }
func (h mergeHeap) Less(i, j int) bool { return h[i].kv.Key < h[j].kv.Key }
func (h mergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)        { *h = append(*h, x.(mergeItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	item := old[len(old)-1]
	*h = old[:len(old)-1]
	return item
}
