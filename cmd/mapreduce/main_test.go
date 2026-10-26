package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MSJawad/mapreduce/internal/storage"
	"github.com/MSJawad/mapreduce/job"
	"github.com/MSJawad/mapreduce/sequential"
)

// The end-to-end tests start real coordinator and worker processes by
// re-executing the test binary with this variable set, which runs main.
const runMainEnv = "MAPREDUCE_TEST_RUN_MAIN"

func TestMain(m *testing.M) {
	if os.Getenv(runMainEnv) == "1" {
		os.Args = append([]string{"mapreduce"}, os.Args[1:]...)
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// slowcount is word count with map tasks slow enough to kill a worker in the
// middle of one.
func init() {
	job.Register(job.Definition{
		Name: "test-slowcount",
		New: func(map[string]string) (*job.Job, error) {
			wc, err := job.New("wordcount", nil)
			if err != nil {
				return nil, err
			}
			slow := job.MapperFunc(func(name string, contents []byte, emit job.Emit) error {
				time.Sleep(time.Second)
				return wc.Mapper.Map(name, contents, emit)
			})
			return &job.Job{Mapper: slow, Reducer: wc.Reducer}, nil
		},
	})
}

func writeInputs(t *testing.T, n int) []string {
	t.Helper()
	dir := t.TempDir()
	var files []string
	for i := range n {
		path := filepath.Join(dir, fmt.Sprintf("book-%d.txt", i))
		text := strings.Repeat(fmt.Sprintf("Chapter %d. It was the best of times, it was the worst of times; book %c.\n", i, 'a'+i), 50+i)
		if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
		files = append(files, path)
	}
	return files
}

func TestUsageErrors(t *testing.T) {
	var out bytes.Buffer
	for _, args := range [][]string{
		nil,
		{"bogus"},
		{"coordinator", "in.txt"},
		{"local", "-out", t.TempDir()},
		{"worker", "extra"},
		{"local", "-no-such-flag"},
		{"sequential", "-param", "novalue", "-out", "x", "in.txt"},
	} {
		if err := run(t.Context(), args, &out, &out); !errors.Is(err, errUsage) {
			t.Errorf("run(%q) = %v, want a usage error", args, err)
		}
	}
}

func TestJobsCommand(t *testing.T) {
	var out bytes.Buffer
	if err := run(t.Context(), []string{"jobs"}, &out, &out); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"wordcount", "index", "grep", "-param pattern="} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("jobs output lacks %q:\n%s", want, out.String())
		}
	}
}

func TestLocalMatchesSequential(t *testing.T) {
	inputs := writeInputs(t, 4)
	glob := filepath.Join(filepath.Dir(inputs[0]), "*.txt")
	outDir := filepath.Join(t.TempDir(), "out")
	seqFile := filepath.Join(t.TempDir(), "seq.txt")
	var logs bytes.Buffer
	args := []string{"-job", "grep", "-param", "pattern=worst of times; book [bc]", "-param", ""}
	if err := run(t.Context(), append([]string{"local", "-workers", "3", "-reduce", "3", "-out", outDir}, append(args, glob)...), &logs, &logs); err != nil {
		t.Fatalf("local: %v\n%s", err, logs.String())
	}
	if err := run(t.Context(), append([]string{"sequential", "-out", seqFile}, append(args, glob)...), &logs, &logs); err != nil {
		t.Fatalf("sequential: %v", err)
	}
	got, err := storage.ReadOutputs(outDir)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := os.ReadFile(seqFile)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(seq), "\n"), "\n")
	if len(got) != len(lines) || len(lines) != 51+52 {
		t.Fatalf("local produced %d records, sequential %d, want %d", len(got), len(lines), 51+52)
	}
	for _, line := range lines {
		k, v, _ := strings.Cut(line, "\t")
		if got[k] != v {
			t.Errorf("key %q: local %q, sequential %q", k, got[k], v)
		}
	}
}

// process is a running copy of the CLI whose log lines can be awaited.
type process struct {
	name string
	cmd  *exec.Cmd
	done chan struct{}
	err  error

	mu    sync.Mutex
	lines []string
	wake  chan struct{}
}

func start(t *testing.T, name string, args ...string) *process {
	t.Helper()
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), runMainEnv+"=1")
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	p := &process{name: name, cmd: cmd, done: make(chan struct{}), wake: make(chan struct{})}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			p.mu.Lock()
			p.lines = append(p.lines, sc.Text())
			close(p.wake)
			p.wake = make(chan struct{})
			p.mu.Unlock()
		}
		p.err = cmd.Wait()
		close(p.done)
	}()
	t.Cleanup(func() {
		cmd.Process.Kill()
		<-p.done
		if t.Failed() {
			t.Logf("%s log:\n%s", p.name, p.log())
		}
	})
	return p
}

func (p *process) log() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return strings.Join(p.lines, "\n")
}

// await blocks until a log line matches re and returns its submatches.
func (p *process) await(t *testing.T, re *regexp.Regexp) []string {
	t.Helper()
	timeout := time.After(30 * time.Second)
	for seen := 0; ; {
		p.mu.Lock()
		lines, wake := p.lines, p.wake
		p.mu.Unlock()
		for ; seen < len(lines); seen++ {
			if m := re.FindStringSubmatch(lines[seen]); m != nil {
				return m
			}
		}
		select {
		case <-wake:
		case <-p.done:
			t.Fatalf("%s exited (%v) before logging %v", p.name, p.err, re)
		case <-timeout:
			t.Fatalf("%s never logged %v", p.name, re)
		}
	}
}

func (p *process) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-p.done:
		return p.err
	case <-time.After(time.Minute):
		t.Fatalf("%s did not exit", p.name)
		return nil
	}
}

// TestKilledWorkerProcess runs a coordinator and three workers as separate
// processes talking over TCP, SIGKILLs a worker in the middle of a map task
// and checks that the job still completes with the right answer.
func TestKilledWorkerProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("starts several processes")
	}
	inputs := writeInputs(t, 6)
	outDir := filepath.Join(t.TempDir(), "out")
	coord := start(t, "coordinator", append([]string{"coordinator",
		"-listen", "127.0.0.1:0", "-job", "test-slowcount", "-reduce", "3", "-out", outDir,
		"-lease", "1s", "-backup-after", "-1s", "-v"}, inputs...)...)
	addr := coord.await(t, regexp.MustCompile(`msg="coordinator listening" addr=(\S+)`))[1]

	victim := start(t, "w0", "worker", "-coordinator", addr, "-id", "w0")
	coord.await(t, regexp.MustCompile(`msg="task assigned" kind=map .* worker=w0`))
	w1 := start(t, "w1", "worker", "-coordinator", addr, "-id", "w1")
	w2 := start(t, "w2", "worker", "-coordinator", addr, "-id", "w2")
	coord.await(t, regexp.MustCompile(`msg="task assigned" kind=map .* worker=w[12]`))
	if err := victim.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}

	if err := coord.wait(t); err != nil {
		t.Fatalf("coordinator exited with %v", err)
	}
	for _, w := range []*process{w1, w2} {
		if err := w.wait(t); err != nil {
			t.Errorf("%s exited with %v", w.name, err)
		}
	}
	log := coord.log()
	for _, want := range []string{`msg="lease expired; task will be re-executed" kind=map`, `worker=w0`, `msg="job complete"`} {
		if !strings.Contains(log, want) {
			t.Errorf("coordinator log lacks %q", want)
		}
	}

	// test-slowcount computes exactly what wordcount does, only slower.
	j, _ := job.New("wordcount", nil)
	want, err := sequential.Run(j, inputs)
	if err != nil {
		t.Fatal(err)
	}
	got, err := storage.ReadOutputs(outDir)
	if err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(got, want) {
		t.Errorf("output differs from the sequential reference:\n got %v\nwant %v", got, want)
	}
}

func TestCoordinatorStopsOnSignal(t *testing.T) {
	inputs := writeInputs(t, 1)
	coord := start(t, "coordinator", "coordinator", "-listen", "127.0.0.1:0", "-out", filepath.Join(t.TempDir(), "out"), inputs[0])
	coord.await(t, regexp.MustCompile(`coordinator listening`))
	coord.cmd.Process.Signal(os.Interrupt)
	err := coord.wait(t)
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Errorf("interrupted coordinator exited with %v, want status 1", err)
	}
	if !strings.Contains(coord.log(), context.Canceled.Error()) {
		t.Errorf("log does not explain the interruption:\n%s", coord.log())
	}
}
