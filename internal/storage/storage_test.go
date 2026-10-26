package storage

import (
	"errors"
	"hash/fnv"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func readAll(t *testing.T, path string) ([]KeyValue, error) {
	t.Helper()
	rr, err := OpenRecords(path)
	if err != nil {
		return nil, err
	}
	defer rr.Close()
	var got []KeyValue
	for {
		kv, ok := rr.Next()
		if !ok {
			return got, rr.Err()
		}
		got = append(got, kv)
	}
}

func TestRecordsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	for name, kvs := range map[string][]KeyValue{
		"empty":  nil,
		"simple": {{"a", "1"}, {"b", "2"}},
		"odd":    {{"", ""}, {"tab\tkey", "new\nline"}, {"\x00binary\xff", strings.Repeat("v", 70000)}},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(dir, name)
			if err := WriteRecords(path, kvs); err != nil {
				t.Fatal(err)
			}
			got, err := readAll(t, path)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, kvs) {
				t.Errorf("read %d records, want %d", len(got), len(kvs))
			}
		})
	}
}

func TestWriteRecordsLeavesNoTempFiles(t *testing.T) {
	dir := t.TempDir()
	if err := WriteRecords(filepath.Join(dir, "part"), []KeyValue{{"k", "v"}}); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 || entries[0].Name() != "part" {
		t.Errorf("directory holds %v, want only part", entries)
	}
}

// Every proper prefix of a valid file must be rejected rather than read as a
// shorter but valid stream.
func TestTruncatedRecordsAreDetected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "full")
	if err := WriteRecords(path, []KeyValue{{"alpha", "1"}, {"beta", "22"}, {"gamma", "333"}}); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	for n := range len(data) {
		cut := filepath.Join(dir, "cut")
		os.WriteFile(cut, data[:n], 0o644)
		if _, err := readAll(t, cut); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("prefix of %d/%d bytes: err = %v, want ErrCorrupt", n, len(data), err)
		}
	}
	os.WriteFile(path, append(data, 0), 0o644)
	if _, err := readAll(t, path); !errors.Is(err, ErrCorrupt) {
		t.Errorf("trailing garbage: err = %v, want ErrCorrupt", err)
	}
}

func TestMergeGroups(t *testing.T) {
	dir := t.TempDir()
	runs := [][]KeyValue{
		{{"apple", "1"}, {"cherry", "2"}, {"cherry", "3"}},
		{},
		{{"apple", "4"}, {"banana", "5"}, {"date", "6"}},
		{{"cherry", "7"}},
	}
	var readers []*RecordReader
	for i, kvs := range runs {
		path := filepath.Join(dir, string(rune('a'+i)))
		if err := WriteRecords(path, kvs); err != nil {
			t.Fatal(err)
		}
		rr, err := OpenRecords(path)
		if err != nil {
			t.Fatal(err)
		}
		defer rr.Close()
		readers = append(readers, rr)
	}
	var keys []string
	groups := map[string][]string{}
	err := MergeGroups(readers, func(key string, values []string) error {
		keys = append(keys, key)
		groups[key] = slices.Sorted(slices.Values(values))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"apple", "banana", "cherry", "date"}; !slices.Equal(keys, want) {
		t.Errorf("keys = %v, want %v", keys, want)
	}
	if want := []string{"2", "3", "7"}; !slices.Equal(groups["cherry"], want) {
		t.Errorf("cherry = %v, want %v", groups["cherry"], want)
	}
}

func TestPartitionIsFNV1a(t *testing.T) {
	for _, key := range []string{"", "a", "hello", "über", strings.Repeat("xyz", 100)} {
		h := fnv.New32a()
		h.Write([]byte(key))
		if got, want := Partition(key, 1000003), int(h.Sum32()%1000003); got != want {
			t.Errorf("Partition(%q) = %d, want %d", key, got, want)
		}
	}
}

func TestPartition(t *testing.T) {
	counts := make([]int, 7)
	for i := range 7000 {
		key := strings.Repeat("k", i%13) + string(rune('a'+i%26)) + string(rune(i))
		p := Partition(key, 7)
		if p != Partition(key, 7) {
			t.Fatal("Partition is not deterministic")
		}
		counts[p]++
	}
	for p, n := range counts {
		if n < 500 {
			t.Errorf("partition %d got only %d of 7000 keys: %v", p, n, counts)
		}
	}
}

func TestOutputCommitAndAbort(t *testing.T) {
	out := t.TempDir()
	os.MkdirAll(TempDir(out), 0o755)

	w, err := CreateOutput(out, 3, 9)
	if err != nil {
		t.Fatal(err)
	}
	w.Write("hello", "1")
	w.Write("world", "two words")
	if _, err := os.Stat(OutputFile(out, 3)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("output visible before commit")
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	got, err := ReadOutputs(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["world"] != "two words" {
		t.Errorf("ReadOutputs = %v", got)
	}

	a, _ := CreateOutput(out, 4, 10)
	a.Write("x", "y")
	a.Abort()
	if _, err := os.Stat(OutputFile(out, 4)); !errors.Is(err, os.ErrNotExist) {
		t.Error("aborted output was committed")
	}
	if entries, _ := os.ReadDir(TempDir(out)); len(entries) != 0 {
		t.Errorf("temp dir not clean: %v", entries)
	}
}

func TestOutputRejectsAmbiguousRecords(t *testing.T) {
	out := t.TempDir()
	os.MkdirAll(TempDir(out), 0o755)
	w, _ := CreateOutput(out, 0, 1)
	defer w.Abort()
	if err := w.Write("a\tb", "v"); err == nil {
		t.Error("accepted a key with a tab")
	}
	if err := w.Write("k", "two\nlines"); err == nil {
		t.Error("accepted a value with a newline")
	}
}

func TestReadOutputsRejectsDuplicateKeys(t *testing.T) {
	out := t.TempDir()
	os.WriteFile(OutputFile(out, 0), []byte("k\t1\n"), 0o644)
	os.WriteFile(OutputFile(out, 1), []byte("k\t2\n"), 0o644)
	if _, err := ReadOutputs(out); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("err = %v, want duplicate key error", err)
	}
}
