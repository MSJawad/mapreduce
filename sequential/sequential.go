// Package sequential runs a job in a single process with none of the
// distributed machinery: no partitioning, no combiner, no intermediate files.
// It is the reference the distributed results are checked against.
package sequential

import (
	"bufio"
	"os"
	"sort"

	"github.com/MSJawad/mapreduce/job"
)

// Run applies j to inputs and returns the reduced value for every key.
func Run(j *job.Job, inputs []string) (map[string]string, error) {
	grouped := map[string][]string{}
	for _, in := range inputs {
		data, err := os.ReadFile(in)
		if err != nil {
			return nil, err
		}
		err = j.Mapper.Map(in, data, func(key, value string) {
			grouped[key] = append(grouped[key], value)
		})
		if err != nil {
			return nil, err
		}
	}
	out := make(map[string]string, len(grouped))
	for key, values := range grouped {
		v, err := j.Reducer.Reduce(key, values)
		if err != nil {
			return nil, err
		}
		out[key] = v
	}
	return out, nil
}

// WriteFile writes results as "key<TAB>value" lines sorted by key, the same
// line format as the distributed output files.
func WriteFile(path string, results map[string]string) error {
	keys := make([]string, 0, len(results))
	for k := range results {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, k := range keys {
		w.WriteString(k)
		w.WriteByte('\t')
		w.WriteString(results[k])
		w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
