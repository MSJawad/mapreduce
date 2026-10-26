package worker

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/MSJawad/mapreduce/internal/storage"
	"github.com/MSJawad/mapreduce/job"
)

func TestMapBufferCombines(t *testing.T) {
	wc, _ := job.New("wordcount", nil)
	b := newMapBuffer(1, wc.Combiner)
	for range 3*combineEvery + 7 {
		b.emit("the", "1")
	}
	b.emit("cat", "1")
	b.emit("cat", "1")
	b.emit("a", "1")
	if n := len(b.parts[0]["the"]); n >= combineEvery {
		t.Errorf("frequent key holds %d values; the combiner should fold them as they arrive", n)
	}
	got, err := b.records(0)
	if err != nil {
		t.Fatal(err)
	}
	want := []storage.KeyValue{{Key: "a", Value: "1"}, {Key: "cat", Value: "2"}, {Key: "the", Value: fmt.Sprint(3*combineEvery + 7)}}
	if !slices.Equal(got, want) {
		t.Errorf("records = %v, want %v", got, want)
	}
}

func TestMapBufferWithoutCombinerKeepsEveryValue(t *testing.T) {
	b := newMapBuffer(3, nil)
	b.emit("k", "x")
	b.emit("j", "y")
	b.emit("k", "x")
	var all []storage.KeyValue
	for r := range 3 {
		kvs, _ := b.records(r)
		if !slices.IsSortedFunc(kvs, func(a, b storage.KeyValue) int { return strings.Compare(a.Key, b.Key) }) {
			t.Errorf("partition %d not sorted: %v", r, kvs)
		}
		for _, kv := range kvs {
			if storage.Partition(kv.Key, 3) != r {
				t.Errorf("key %q in partition %d", kv.Key, r)
			}
		}
		all = append(all, kvs...)
	}
	if len(all) != 3 {
		t.Errorf("got %v, want all three records", all)
	}
}

func TestMapBufferReportsCombinerErrors(t *testing.T) {
	failing := job.ReducerFunc(func(string, []string) (string, error) { return "", errors.New("boom") })
	b := newMapBuffer(1, failing)
	b.emit("k", "1")
	b.emit("k", "2")
	if _, err := b.records(0); err == nil {
		t.Error("combiner error not returned")
	}
	for range combineEvery {
		b.emit("j", "1")
	}
	if b.err == nil {
		t.Error("combiner error during emit not recorded")
	}
}

func TestRunNeedsCoordinator(t *testing.T) {
	if err := Run(t.Context(), Config{}); err == nil {
		t.Error("Run without a coordinator address succeeded")
	}
}

func TestRunGivesUpOnUnreachableCoordinator(t *testing.T) {
	err := Run(t.Context(), Config{Coordinator: "127.0.0.1:1", CoordinatorTimeout: 200 * time.Millisecond})
	if !errors.Is(err, ErrCoordinatorUnavailable) {
		t.Errorf("err = %v, want ErrCoordinatorUnavailable", err)
	}
}
