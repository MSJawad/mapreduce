package job

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

func init() {
	Register(Definition{
		Name:        "wordcount",
		Description: "Count how often each word occurs (case-insensitive).",
		New: func(map[string]string) (*Job, error) {
			sum := ReducerFunc(sumCounts)
			return &Job{Mapper: MapperFunc(mapWordCount), Reducer: sum, Combiner: sum}, nil
		},
	})
	Register(Definition{
		Name:        "index",
		Description: "Build an inverted index: for each word, the input files that contain it.",
		New: func(map[string]string) (*Job, error) {
			return &Job{Mapper: MapperFunc(mapIndex), Reducer: ReducerFunc(reduceIndex)}, nil
		},
	})
	Register(Definition{
		Name:        "grep",
		Description: "Distributed grep: every line that matches a regular expression, keyed by file:line.",
		Params: map[string]string{
			"pattern": "regular expression to match (RE2 syntax, required)",
		},
		New: newGrep,
	})
}

// EachWord calls fn for every word in text, lower-cased, where a word is a
// maximal run of Unicode letters. Everything else (digits, punctuation,
// whitespace) separates words.
func EachWord(text []byte, fn func(word string)) {
	start := -1
	for i := 0; i < len(text); {
		r, size := rune(text[i]), 1
		if r >= utf8.RuneSelf {
			r, size = utf8.DecodeRune(text[i:])
		}
		if unicode.IsLetter(r) {
			if start < 0 {
				start = i
			}
		} else if start >= 0 {
			fn(strings.ToLower(string(text[start:i])))
			start = -1
		}
		i += size
	}
	if start >= 0 {
		fn(strings.ToLower(string(text[start:])))
	}
}

func mapWordCount(_ string, contents []byte, emit Emit) error {
	EachWord(contents, func(w string) { emit(w, "1") })
	return nil
}

func sumCounts(key string, values []string) (string, error) {
	var total int64
	for _, v := range values {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return "", fmt.Errorf("key %q: bad count %q", key, v)
		}
		total += n
	}
	return strconv.FormatInt(total, 10), nil
}

func mapIndex(filename string, contents []byte, emit Emit) error {
	seen := map[string]bool{}
	EachWord(contents, func(w string) {
		if !seen[w] {
			seen[w] = true
			emit(w, filename)
		}
	})
	return nil
}

// reduceIndex outputs "<number of files> <file>,<file>,..." with the files
// sorted, so the result does not depend on the order values arrive in.
func reduceIndex(_ string, files []string) (string, error) {
	files = slices.Clone(files)
	slices.Sort(files)
	files = slices.Compact(files)
	return strconv.Itoa(len(files)) + " " + strings.Join(files, ","), nil
}

func newGrep(params map[string]string) (*Job, error) {
	pattern := params["pattern"]
	if pattern == "" {
		return nil, errors.New("missing required parameter pattern")
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("bad pattern: %w", err)
	}
	mapper := func(filename string, contents []byte, emit Emit) error {
		if len(contents) == 0 {
			return nil
		}
		contents = bytes.TrimSuffix(contents, []byte("\n"))
		for i, line := range bytes.Split(contents, []byte("\n")) {
			line = bytes.TrimSuffix(line, []byte("\r"))
			if re.Match(line) {
				emit(filename+":"+strconv.Itoa(i+1), string(line))
			}
		}
		return nil
	}
	// Each file:line key is emitted at most once per input file, so the
	// reducer only has to pass the line through.
	reducer := func(_ string, lines []string) (string, error) { return lines[0], nil }
	return &Job{Mapper: MapperFunc(mapper), Reducer: ReducerFunc(reducer)}, nil
}
