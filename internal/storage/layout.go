// Package storage owns everything a job writes to disk: where files live, the
// intermediate record format, merging sorted runs and committing final output.
//
// All files are written to a temporary name and renamed into place only once
// complete, so a reader never observes a partially written file under its
// final name, no matter where a writer crashes.
package storage

import (
	"fmt"
	"path/filepath"
)

// KeyValue is one intermediate record.
type KeyValue struct {
	Key   string
	Value string
}

// Partition picks the reduce partition (0..n-1) that owns key, using the
// 32-bit FNV-1a hash of the key.
func Partition(key string, n int) int {
	h := uint32(2166136261)
	for i := range len(key) {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return int(h % uint32(n))
}

// OutputFile is the committed output of reduce partition r.
func OutputFile(outDir string, r int) string {
	return filepath.Join(outDir, fmt.Sprintf("part-%05d", r))
}

// IntermediateDir holds every map attempt's output. It is removed when the job
// succeeds.
func IntermediateDir(outDir string) string {
	return filepath.Join(outDir, "_intermediate")
}

// TempDir holds reduce output that has not been committed yet. It lives inside
// outDir so that committing is a rename within one filesystem.
func TempDir(outDir string) string {
	return filepath.Join(outDir, "_tmp")
}

// MapAttemptDir is where one attempt of a map task writes its partitions.
// Every attempt gets its own directory, so concurrent attempts of the same
// task (after a lease expiry or for a backup) never touch each other's files,
// and reducers read only from the attempt the coordinator accepted.
func MapAttemptDir(outDir string, mapTask, attempt int) string {
	return filepath.Join(IntermediateDir(outDir), fmt.Sprintf("map-%05d-attempt-%d", mapTask, attempt))
}

// PartitionFile is the file inside a map attempt directory that holds the
// records for reduce partition r.
func PartitionFile(attemptDir string, r int) string {
	return filepath.Join(attemptDir, fmt.Sprintf("part-%05d", r))
}
