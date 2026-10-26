package cluster_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MSJawad/mapreduce/cluster"
	"github.com/MSJawad/mapreduce/coordinator"
	"github.com/MSJawad/mapreduce/internal/storage"
	"github.com/MSJawad/mapreduce/job"
	"github.com/MSJawad/mapreduce/sequential"
	"github.com/MSJawad/mapreduce/worker"
)

func init() {
	job.Register(job.Definition{
		Name: "test-failing-map",
		New: func(map[string]string) (*job.Job, error) {
			return &job.Job{
				Mapper: job.MapperFunc(func(name string, _ []byte, emit job.Emit) error {
					if strings.Contains(name, "poison") {
						return errors.New("cannot digest " + filepath.Base(name))
					}
					emit("ok", "1")
					return nil
				}),
				Reducer: job.ReducerFunc(func(string, []string) (string, error) { return "1", nil }),
			}, nil
		},
	})
	job.Register(job.Definition{
		Name: "test-panicking-reduce",
		New: func(map[string]string) (*job.Job, error) {
			return &job.Job{
				Mapper:  job.MapperFunc(func(_ string, _ []byte, emit job.Emit) error { emit("k", "v"); return nil }),
				Reducer: job.ReducerFunc(func(string, []string) (string, error) { panic("reducer bug") }),
			}, nil
		},
	})
}

var vocabulary = strings.Fields(`the of and to a in that is was he for it with as his on be at by
	had not are but from or have an they which one you were her all she there would their we him
	been has when who will more no if out so said what up its about into than them can only other
	MapReduce Coordinator Worker lease partition shuffle Straggler backup commit rename über naïve`)

// writeCorpus writes files of pseudo-random text with a skewed word
// distribution, mixed case and punctuation, and returns their paths and total
// size. The same seed always produces the same corpus.
func writeCorpus(t testing.TB, files, wordsPerFile int, seed uint64) ([]string, int64) {
	t.Helper()
	dir := t.TempDir()
	rng := rand.New(rand.NewPCG(seed, seed))
	zipf := rand.NewZipf(rng, 1.2, 1, uint64(len(vocabulary)-1))
	var paths []string
	var total int64
	for f := range files {
		var b strings.Builder
		for w := range wordsPerFile {
			word := vocabulary[zipf.Uint64()]
			if rng.IntN(10) == 0 {
				word = strings.ToUpper(word[:1]) + word[1:]
			}
			b.WriteString(word)
			switch {
			case w%17 == 16:
				b.WriteString(".\n")
			case rng.IntN(8) == 0:
				b.WriteString(", ")
			default:
				b.WriteByte(' ')
			}
		}
		path := filepath.Join(dir, fmt.Sprintf("doc-%03d.txt", f))
		if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
		total += int64(b.Len())
	}
	return paths, total
}

func jobConfig(t testing.TB, name string, params map[string]string, inputs []string, nReduce int) coordinator.Config {
	return coordinator.Config{
		Job:       name,
		Params:    params,
		Inputs:    inputs,
		NReduce:   nReduce,
		OutputDir: filepath.Join(t.TempDir(), "out"),
	}
}

func run(t *testing.T, opts cluster.Options) cluster.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res, err := cluster.Run(ctx, opts)
	if err != nil {
		t.Fatalf("cluster.Run: %v (stats %v)", err, res.Stats)
	}
	return res
}

// checkOutput compares the job's output with the sequential reference and
// checks that nothing but the committed part files is left behind.
func checkOutput(t *testing.T, cfg coordinator.Config) {
	t.Helper()
	j, err := job.New(cfg.Job, cfg.Params)
	if err != nil {
		t.Fatal(err)
	}
	want, err := sequential.Run(j, cfg.Inputs)
	if err != nil {
		t.Fatal(err)
	}
	got, err := storage.ReadOutputs(cfg.OutputDir)
	if err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(got, want) {
		diffs := 0
		for k, v := range want {
			if got[k] != v && diffs < 5 {
				t.Errorf("key %q: got %q, want %q", k, got[k], v)
				diffs++
			}
		}
		t.Fatalf("output has %d keys, reference has %d", len(got), len(want))
	}
	entries, err := os.ReadDir(cfg.OutputDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != cfg.NReduce {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("output directory holds %v, want exactly %d part files", names, cfg.NReduce)
	}
}

func TestJobsMatchSequentialReference(t *testing.T) {
	inputs, _ := writeCorpus(t, 9, 4000, 1)
	for _, tc := range []struct {
		job    string
		params map[string]string
	}{
		{"wordcount", nil},
		{"index", nil},
		{"grep", map[string]string{"pattern": `(?i)\bstraggler\b.*\bbackup\b`}},
	} {
		t.Run(tc.job, func(t *testing.T) {
			t.Parallel()
			cfg := jobConfig(t, tc.job, tc.params, inputs, 5)
			res := run(t, cluster.Options{Job: cfg, Workers: 4})
			checkOutput(t, cfg)
			if st := res.Stats; st.Attempts != st.MapTasks+st.ReduceTasks || st.LeaseExpirations+st.Failures+st.Backups != 0 {
				t.Errorf("fault-free run did unexpected work: %v", st)
			}
			for i, err := range res.WorkerErrors {
				if err != nil {
					t.Errorf("worker %d: %v", i, err)
				}
			}
		})
	}
}

func TestMoreWorkersThanTasks(t *testing.T) {
	inputs, _ := writeCorpus(t, 2, 500, 2)
	cfg := jobConfig(t, "wordcount", nil, inputs, 1)
	run(t, cluster.Options{Job: cfg, Workers: 8})
	checkOutput(t, cfg)
}

func TestSingleWorker(t *testing.T) {
	inputs, _ := writeCorpus(t, 5, 500, 3)
	cfg := jobConfig(t, "index", nil, inputs, 3)
	run(t, cluster.Options{Job: cfg, Workers: 1})
	checkOutput(t, cfg)
}

// faultOnce returns a hook factory under which the first worker to start a
// task of the given kind suffers the fault, and records which worker it was.
func faultOnce(victim *atomic.Int32, kind string) func(t worker.TaskInfo) bool {
	victim.Store(-1)
	return func(t worker.TaskInfo) bool {
		if t.Kind != kind {
			return false
		}
		var i int32
		fmt.Sscanf(t.Worker, "worker-%d", &i)
		return victim.CompareAndSwap(-1, i)
	}
}

func checkVictim(t *testing.T, res cluster.Result, victim *atomic.Int32, want error) {
	t.Helper()
	v := victim.Load()
	if v < 0 {
		t.Fatal("no worker hit the injected fault")
	}
	for i, err := range res.WorkerErrors {
		if int32(i) == v {
			if !errors.Is(err, want) {
				t.Errorf("victim worker %d returned %v, want %v", i, err, want)
			}
		} else if err != nil {
			t.Errorf("healthy worker %d returned %v", i, err)
		}
	}
	if res.Stats.LeaseExpirations < 1 {
		t.Errorf("no lease expired; the lost task was not detected: %v", res.Stats)
	}
}

// A worker dies in the middle of a task: it stops renewing its lease and never
// reports. The task must be re-executed elsewhere once the lease expires.
func TestWorkerKilledMidTask(t *testing.T) {
	inputs, _ := writeCorpus(t, 8, 3000, 4)
	for _, kind := range []string{"map", "reduce"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			var victim atomic.Int32
			hit := faultOnce(&victim, kind)
			cfg := jobConfig(t, "wordcount", nil, inputs, 4)
			cfg.LeaseTimeout = 300 * time.Millisecond
			cfg.BackupAfter = -1
			res := run(t, cluster.Options{
				Job:     cfg,
				Workers: 3,
				WorkerHooks: func(_ int, kill func()) worker.Hooks {
					return worker.Hooks{TaskStarted: func(ctx context.Context, t worker.TaskInfo) {
						if hit(t) {
							kill()
							<-ctx.Done()
						}
					}}
				},
			})
			checkOutput(t, cfg)
			checkVictim(t, res, &victim, context.Canceled)
		})
	}
}

// A worker crashes after part of its output is on disk but before it commits.
// The partial files must never be read or end up in the output.
func TestWorkerCrashesAfterPartialOutput(t *testing.T) {
	inputs, _ := writeCorpus(t, 8, 3000, 5)
	for _, kind := range []string{"map", "reduce"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			var victim atomic.Int32
			hit := faultOnce(&victim, kind)
			cfg := jobConfig(t, "index", nil, inputs, 4)
			cfg.LeaseTimeout = 300 * time.Millisecond
			cfg.BackupAfter = -1
			res := run(t, cluster.Options{
				Job:     cfg,
				Workers: 3,
				WorkerHooks: func(int, func()) worker.Hooks {
					return worker.Hooks{PartialOutput: func(t worker.TaskInfo) error {
						if hit(t) {
							return errors.New("power cut")
						}
						return nil
					}}
				},
			})
			checkOutput(t, cfg)
			checkVictim(t, res, &victim, worker.ErrCrashed)
		})
	}
}

// A worker stays alive and keeps renewing its leases but runs far slower than
// the others. Backup attempts must finish the job without waiting for it.
func TestSlowWorkerIsBackedUp(t *testing.T) {
	inputs, _ := writeCorpus(t, 8, 3000, 6)
	const slowness = 30 * time.Second
	var victim atomic.Int32
	victim.Store(-1)
	cfg := jobConfig(t, "wordcount", nil, inputs, 4)
	cfg.LeaseTimeout = 600 * time.Millisecond
	cfg.BackupAfter = 150 * time.Millisecond
	start := time.Now()
	res := run(t, cluster.Options{
		Job:     cfg,
		Workers: 3,
		WorkerHooks: func(i int, _ func()) worker.Hooks {
			return worker.Hooks{TaskStarted: func(ctx context.Context, _ worker.TaskInfo) {
				if victim.CompareAndSwap(-1, int32(i)) || victim.Load() == int32(i) {
					select {
					case <-ctx.Done():
					case <-time.After(slowness):
					}
				}
			}}
		},
	})
	checkOutput(t, cfg)
	if elapsed := time.Since(start); elapsed > slowness/3 {
		t.Errorf("job took %v; the slow worker held it up", elapsed)
	}
	if res.Stats.Backups < 1 {
		t.Errorf("no backup attempts: %v", res.Stats)
	}
	for i, err := range res.WorkerErrors {
		if err != nil {
			t.Errorf("worker %d returned %v, want a clean exit", i, err)
		}
	}
}

func TestMapErrorsFailTheJob(t *testing.T) {
	dir := t.TempDir()
	good, poison := filepath.Join(dir, "good.txt"), filepath.Join(dir, "poison.txt")
	os.WriteFile(good, []byte("x"), 0o644)
	os.WriteFile(poison, []byte("x"), 0o644)
	cfg := jobConfig(t, "test-failing-map", nil, []string{good, poison}, 2)
	cfg.MaxAttempts = 3
	res, err := cluster.Run(context.Background(), cluster.Options{Job: cfg, Workers: 2})
	if err == nil || !strings.Contains(err.Error(), "cannot digest poison.txt") {
		t.Fatalf("err = %v, want the mapper's error", err)
	}
	if res.Stats.Failures != 3 {
		t.Errorf("Failures = %d, want 3 attempts before giving up", res.Stats.Failures)
	}
}

func TestReducerPanicIsReportedNotFatal(t *testing.T) {
	inputs, _ := writeCorpus(t, 2, 10, 7)
	cfg := jobConfig(t, "test-panicking-reduce", nil, inputs, 1)
	cfg.MaxAttempts = 2
	res, err := cluster.Run(context.Background(), cluster.Options{Job: cfg, Workers: 2})
	if err == nil || !strings.Contains(err.Error(), "panic: reducer bug") {
		t.Fatalf("err = %v, want the reducer panic reported as a task failure", err)
	}
	for i, werr := range res.WorkerErrors {
		if werr != nil {
			t.Errorf("worker %d died: %v", i, werr)
		}
	}
}

// BenchmarkWordCount measures end-to-end throughput (input bytes per second)
// for a fixed corpus as the number of workers grows.
func BenchmarkWordCount(b *testing.B) {
	inputs, size := writeCorpus(b, 32, 250_000, 42)
	for _, workers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			b.SetBytes(size)
			for b.Loop() {
				cfg := jobConfig(b, "wordcount", nil, inputs, 8)
				if _, err := cluster.Run(context.Background(), cluster.Options{Job: cfg, Workers: workers}); err != nil {
					b.Fatal(err)
				}
				os.RemoveAll(cfg.OutputDir)
			}
		})
	}
}
