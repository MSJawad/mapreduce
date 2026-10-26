// Package job defines the MapReduce programming model (Mapper, Reducer and an
// optional Combiner) and a registry of the jobs compiled into the binary.
//
// Coordinators and workers refer to a job by its registered name plus a small
// set of string parameters, so the same job definition runs on every process
// without shipping code over the network.
package job

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
)

// Emit records one intermediate key/value pair produced by a Mapper.
type Emit func(key, value string)

// Mapper turns one input file into intermediate key/value pairs.
//
// Map must be deterministic: re-executing it on the same input has to emit the
// same pairs, because the framework may run a task more than once (after a
// worker failure or as a backup for a straggler) and keeps whichever attempt
// finishes first.
type Mapper interface {
	Map(filename string, contents []byte, emit Emit) error
}

// Reducer merges all values emitted for one key into a single output value.
//
// Reduce is called once per distinct key with the values in no particular
// order. Like Map, it must be deterministic. It must not keep a reference to
// values after returning, since the framework reuses the slice.
type Reducer interface {
	Reduce(key string, values []string) (string, error)
}

// MapperFunc adapts an ordinary function to the Mapper interface.
type MapperFunc func(filename string, contents []byte, emit Emit) error

// Map calls f.
func (f MapperFunc) Map(filename string, contents []byte, emit Emit) error {
	return f(filename, contents, emit)
}

// ReducerFunc adapts an ordinary function to the Reducer interface.
type ReducerFunc func(key string, values []string) (string, error)

// Reduce calls f.
func (f ReducerFunc) Reduce(key string, values []string) (string, error) {
	return f(key, values)
}

// Job is a runnable MapReduce program.
type Job struct {
	Mapper  Mapper
	Reducer Reducer

	// Combiner, when set, pre-aggregates each map task's output per key before
	// it is written, which can shrink intermediate data dramatically (word
	// count is the classic example). It may be applied repeatedly, to values
	// it produced itself, so its output must be a valid input for both
	// Combiner and Reducer, and applying it must not change the final
	// result: it only suits associative and commutative reductions.
	Combiner Reducer
}

// Definition describes a registered job.
type Definition struct {
	Name        string
	Description string

	// Params lists the parameters the job accepts, mapped to a one-line
	// description. Unknown parameters are rejected.
	Params map[string]string

	// New builds the job from its parameters. It is called on the
	// coordinator to validate the parameters up front and on every worker for
	// every task, so it should be cheap.
	New func(params map[string]string) (*Job, error)
}

var (
	registryMu sync.RWMutex
	registry   = map[string]Definition{}
)

// Register adds a job definition to the registry. It panics if the definition
// is incomplete or the name is already taken, since both are programming
// errors that should fail at startup.
func Register(d Definition) {
	if d.Name == "" || d.New == nil {
		panic("job: Register needs a name and a constructor")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, dup := registry[d.Name]; dup {
		panic(fmt.Sprintf("job: %q registered twice", d.Name))
	}
	registry[d.Name] = d
}

// Lookup returns the definition registered under name.
func Lookup(name string) (Definition, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	d, ok := registry[name]
	return d, ok
}

// List returns every registered definition, sorted by name.
func List() []Definition {
	registryMu.RLock()
	defer registryMu.RUnlock()
	defs := slices.Collect(maps.Values(registry))
	sort.Slice(defs, func(i, j int) bool { return defs[i].Name < defs[j].Name })
	return defs
}

// New looks up the named job, checks its parameters and builds it.
func New(name string, params map[string]string) (*Job, error) {
	d, ok := Lookup(name)
	if !ok {
		var names []string
		for _, d := range List() {
			names = append(names, d.Name)
		}
		return nil, fmt.Errorf("unknown job %q (available: %s)", name, strings.Join(names, ", "))
	}
	for k := range params {
		if _, known := d.Params[k]; !known {
			return nil, fmt.Errorf("job %q does not take parameter %q", name, k)
		}
	}
	j, err := d.New(params)
	if err != nil {
		return nil, fmt.Errorf("job %q: %w", name, err)
	}
	if j.Mapper == nil || j.Reducer == nil {
		return nil, fmt.Errorf("job %q: constructor returned no mapper or reducer", name)
	}
	return j, nil
}
