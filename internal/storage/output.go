package storage

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// OutputWriter writes one reduce partition's output as "key<TAB>value" lines
// to a temporary file. Commit makes it visible under its final name.
type OutputWriter struct {
	f     *os.File
	w     *bufio.Writer
	final string
	n     int
}

// CreateOutput starts the output of one attempt of reduce partition r.
func CreateOutput(outDir string, r, attempt int) (*OutputWriter, error) {
	tmp := filepath.Join(TempDir(outDir), fmt.Sprintf("part-%05d.attempt-%d", r, attempt))
	f, err := os.Create(tmp)
	if err != nil {
		return nil, err
	}
	return &OutputWriter{f: f, w: bufio.NewWriterSize(f, 64<<10), final: OutputFile(outDir, r)}, nil
}

// Write appends one output record. Keys may not contain tabs or newlines and
// values may not contain newlines, since either would make the line-based
// output ambiguous.
func (o *OutputWriter) Write(key, value string) error {
	if strings.ContainsAny(key, "\t\n") {
		return fmt.Errorf("output key %q contains a tab or newline", key)
	}
	if strings.Contains(value, "\n") {
		return fmt.Errorf("output value for key %q contains a newline", key)
	}
	o.w.WriteString(key)
	o.w.WriteByte('\t')
	o.w.WriteString(value)
	o.n++
	return o.w.WriteByte('\n')
}

// Records reports how many records have been written so far.
func (o *OutputWriter) Records() int { return o.n }

// Commit flushes the output to stable storage and atomically renames it to
// its final name. If several attempts of the same partition commit, each
// rename replaces the file whole, and since reduce is deterministic they all
// carry the same content.
func (o *OutputWriter) Commit() error {
	err := o.w.Flush()
	if err == nil {
		err = o.f.Sync()
	}
	if cerr := o.f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(o.f.Name(), o.final)
	}
	if err != nil {
		os.Remove(o.f.Name())
	}
	return err
}

// Abort discards the uncommitted output.
func (o *OutputWriter) Abort() {
	o.f.Close()
	os.Remove(o.f.Name())
}

// ReadOutputs reads every committed part file in outDir into a map from key
// to value. A key that appears twice is an error, because each key belongs to
// exactly one partition.
func ReadOutputs(outDir string) (map[string]string, error) {
	files, err := filepath.Glob(filepath.Join(outDir, "part-*"))
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no output files in %s", outDir)
	}
	out := map[string]string{}
	for _, name := range files {
		f, err := os.Open(name)
		if err != nil {
			return nil, err
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 64<<10), maxFieldSize)
		for sc.Scan() {
			key, value, ok := strings.Cut(sc.Text(), "\t")
			if !ok {
				f.Close()
				return nil, fmt.Errorf("%s: malformed line %q", name, sc.Text())
			}
			if _, dup := out[key]; dup {
				f.Close()
				return nil, fmt.Errorf("%s: duplicate key %q", name, key)
			}
			out[key] = value
		}
		err = errors.Join(sc.Err(), f.Close())
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Close releases the file without committing or removing it, leaving the
// partial output behind. Fault-injection tests use it to mimic a crash.
func (o *OutputWriter) Close() {
	o.f.Close()
}
