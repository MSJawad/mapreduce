package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Intermediate file format:
//
//	magic "MRKV1\n"
//	repeated: uvarint(len(key)+1) key uvarint(len(value)) value
//	uvarint(0) uvarint(record count)
//
// The trailer lets a reader tell a complete file from a truncated or corrupt
// one instead of silently dropping records.
const (
	recordMagic  = "MRKV1\n"
	maxFieldSize = 1 << 30
)

// ErrCorrupt reports an intermediate file that is truncated or malformed.
var ErrCorrupt = errors.New("corrupt intermediate file")

// WriteRecords writes kvs, in order, to path. The file appears under path only
// once it is complete.
func WriteRecords(path string, kvs []KeyValue) error {
	return writeAtomically(path, func(w *bufio.Writer) error {
		var buf [binary.MaxVarintLen64]byte
		putUvarint := func(v uint64) {
			n := binary.PutUvarint(buf[:], v)
			w.Write(buf[:n])
		}
		w.WriteString(recordMagic)
		for _, kv := range kvs {
			putUvarint(uint64(len(kv.Key)) + 1)
			w.WriteString(kv.Key)
			putUvarint(uint64(len(kv.Value)))
			w.WriteString(kv.Value)
		}
		putUvarint(0)
		putUvarint(uint64(len(kvs)))
		return nil
	})
}

// writeAtomically creates a temporary file next to path, fills it through
// fill, and renames it to path.
func writeAtomically(path string, fill func(*bufio.Writer) error) (err error) {
	f, err := os.CreateTemp(filepath.Dir(path), ".tmp-"+filepath.Base(path)+"-*")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(f.Name())
		}
	}()
	w := bufio.NewWriterSize(f, 64<<10)
	if err := fill(w); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

// RecordReader streams the records of one intermediate file.
type RecordReader struct {
	path  string
	f     *os.File
	r     *bufio.Reader
	count uint64
	err   error
	done  bool
}

// OpenRecords opens an intermediate file written by WriteRecords.
func OpenRecords(path string) (*RecordReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	rr := &RecordReader{path: path, f: f, r: bufio.NewReaderSize(f, 64<<10)}
	magic := make([]byte, len(recordMagic))
	if _, err := io.ReadFull(rr.r, magic); err != nil || string(magic) != recordMagic {
		f.Close()
		return nil, fmt.Errorf("%s: %w: bad header", path, ErrCorrupt)
	}
	return rr, nil
}

// Next returns the next record. It returns false at the end of the file or on
// error; check Err to tell the two apart.
func (rr *RecordReader) Next() (KeyValue, bool) {
	if rr.done || rr.err != nil {
		return KeyValue{}, false
	}
	keyLen, err := binary.ReadUvarint(rr.r)
	if err != nil {
		return rr.fail("truncated record")
	}
	if keyLen == 0 {
		rr.done = true
		n, err := binary.ReadUvarint(rr.r)
		if err != nil || n != rr.count {
			return rr.fail("record count mismatch")
		}
		if _, err := rr.r.ReadByte(); err != io.EOF {
			return rr.fail("data after trailer")
		}
		return KeyValue{}, false
	}
	key, ok := rr.field(keyLen - 1)
	if !ok {
		return rr.fail("truncated key")
	}
	valueLen, err := binary.ReadUvarint(rr.r)
	if err != nil {
		return rr.fail("truncated record")
	}
	value, ok := rr.field(valueLen)
	if !ok {
		return rr.fail("truncated value")
	}
	rr.count++
	return KeyValue{Key: key, Value: value}, true
}

func (rr *RecordReader) field(n uint64) (string, bool) {
	if n > maxFieldSize {
		return "", false
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(rr.r, b); err != nil {
		return "", false
	}
	return string(b), true
}

func (rr *RecordReader) fail(why string) (KeyValue, bool) {
	rr.err = fmt.Errorf("%s: %w: %s", rr.path, ErrCorrupt, why)
	return KeyValue{}, false
}

// Err returns the error that stopped Next, if any.
func (rr *RecordReader) Err() error { return rr.err }

// Close releases the file.
func (rr *RecordReader) Close() error { return rr.f.Close() }
