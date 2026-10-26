package worker

import (
	"fmt"
	"maps"
	"slices"

	"github.com/MSJawad/mapreduce/internal/storage"
	"github.com/MSJawad/mapreduce/job"
)

// combineEvery is how many values a key may accumulate before the combiner
// folds them into one. It keeps memory flat for very frequent keys.
const combineEvery = 256

// mapBuffer collects one map task's output, grouped by partition and key.
type mapBuffer struct {
	combiner job.Reducer
	parts    []map[string][]string
	err      error // first combiner error
}

func newMapBuffer(nReduce int, combiner job.Reducer) *mapBuffer {
	b := &mapBuffer{combiner: combiner, parts: make([]map[string][]string, nReduce)}
	for r := range b.parts {
		b.parts[r] = map[string][]string{}
	}
	return b
}

func (b *mapBuffer) emit(key, value string) {
	if b.err != nil {
		return
	}
	part := b.parts[storage.Partition(key, len(b.parts))]
	values := part[key]
	values = append(values, value)
	if b.combiner != nil && len(values) >= combineEvery {
		v, err := b.combiner.Reduce(key, values)
		if err != nil {
			b.err = fmt.Errorf("combine %q: %w", key, err)
			return
		}
		values = append(values[:0], v)
	}
	part[key] = values
}

// records returns partition r sorted by key. With a combiner, each key's
// values are combined into a single record.
func (b *mapBuffer) records(r int) ([]storage.KeyValue, error) {
	part := b.parts[r]
	var out []storage.KeyValue
	for _, key := range slices.Sorted(maps.Keys(part)) {
		values := part[key]
		if b.combiner != nil && len(values) > 1 {
			v, err := b.combiner.Reduce(key, values)
			if err != nil {
				return nil, fmt.Errorf("combine %q: %w", key, err)
			}
			values = []string{v}
		}
		for _, v := range values {
			out = append(out, storage.KeyValue{Key: key, Value: v})
		}
	}
	return out, nil
}
