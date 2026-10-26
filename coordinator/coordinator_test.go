package coordinator

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MSJawad/mapreduce/internal/protocol"
	"github.com/MSJawad/mapreduce/internal/storage"
)

// These tests drive the coordinator's RPC handlers directly, playing the part
// of workers, to pin down the scheduling state machine.

func inputs(t *testing.T, n int) []string {
	t.Helper()
	dir := t.TempDir()
	var files []string
	for i := range n {
		f := filepath.Join(dir, "in-"+string(rune('a'+i)))
		if err := os.WriteFile(f, []byte("some words"), 0o644); err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	return files
}

func newTestCoordinator(t *testing.T, cfg Config) *Coordinator {
	t.Helper()
	if cfg.Job == "" {
		cfg.Job = "wordcount"
	}
	if cfg.Inputs == nil {
		cfg.Inputs = inputs(t, 2)
	}
	if cfg.NReduce == 0 {
		cfg.NReduce = 2
	}
	if cfg.OutputDir == "" {
		cfg.OutputDir = filepath.Join(t.TempDir(), "out")
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func request(t *testing.T, c *Coordinator, worker string) protocol.RequestTaskReply {
	t.Helper()
	var reply protocol.RequestTaskReply
	if err := c.requestTask(&protocol.RequestTaskArgs{Version: protocol.Version, WorkerID: worker}, &reply); err != nil {
		t.Fatal(err)
	}
	return reply
}

func mustRun(t *testing.T, c *Coordinator, worker string, kind protocol.Kind) protocol.Task {
	t.Helper()
	reply := request(t, c, worker)
	if reply.Action != protocol.Run || reply.Task.Kind != kind {
		t.Fatalf("%s got action %v task %+v, want a %v task", worker, reply.Action, reply.Task, kind)
	}
	return reply.Task
}

func ref(worker string, task protocol.Task) protocol.TaskRef {
	return protocol.TaskRef{WorkerID: worker, Kind: task.Kind, ID: task.ID, Attempt: task.Attempt}
}

func report(t *testing.T, c *Coordinator, worker string, task protocol.Task, errMsg string) bool {
	t.Helper()
	var reply protocol.ReportTaskReply
	if err := c.reportTask(&protocol.ReportTaskArgs{TaskRef: ref(worker, task), Error: errMsg}, &reply); err != nil {
		t.Fatal(err)
	}
	return reply.Accepted
}

func renew(t *testing.T, c *Coordinator, worker string, task protocol.Task) bool {
	t.Helper()
	var reply protocol.RenewLeaseReply
	if err := c.renewLease(&protocol.RenewLeaseArgs{TaskRef: ref(worker, task)}, &reply); err != nil {
		t.Fatal(err)
	}
	return reply.Continue
}

func isDone(c *Coordinator) bool {
	select {
	case <-c.Done():
		return true
	default:
		return false
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	t.Parallel()
	in := inputs(t, 1)
	nonEmpty := t.TempDir()
	os.WriteFile(filepath.Join(nonEmpty, "old"), nil, 0o644)
	for _, tc := range []struct {
		name string
		cfg  Config
		want string
	}{
		{"unknown job", Config{Job: "nope", Inputs: in, NReduce: 1, OutputDir: t.TempDir()}, "unknown job"},
		{"bad params", Config{Job: "grep", Inputs: in, NReduce: 1, OutputDir: t.TempDir()}, "pattern"},
		{"no reduce", Config{Job: "wordcount", Inputs: in, OutputDir: t.TempDir()}, "NReduce"},
		{"no inputs", Config{Job: "wordcount", NReduce: 1, OutputDir: t.TempDir()}, "no input"},
		{"missing input", Config{Job: "wordcount", Inputs: []string{"/does/not/exist"}, NReduce: 1, OutputDir: t.TempDir()}, "no such file"},
		{"dir input", Config{Job: "wordcount", Inputs: []string{t.TempDir()}, NReduce: 1, OutputDir: t.TempDir()}, "directory"},
		{"no output", Config{Job: "wordcount", Inputs: in, NReduce: 1}, "output"},
		{"used output", Config{Job: "wordcount", Inputs: in, NReduce: 1, OutputDir: nonEmpty}, "not empty"},
		{"negative lease", Config{Job: "wordcount", Inputs: in, NReduce: 1, OutputDir: t.TempDir(), LeaseTimeout: -time.Second}, "LeaseTimeout"},
		{"tiny lease", Config{Job: "wordcount", Inputs: in, NReduce: 1, OutputDir: t.TempDir(), LeaseTimeout: time.Nanosecond}, "LeaseTimeout"},
		{"negative attempts", Config{Job: "wordcount", Inputs: in, NReduce: 1, OutputDir: t.TempDir(), MaxAttempts: -1}, "MaxAttempts"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(tc.cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("New error = %v, want it to mention %q", err, tc.want)
			}
			if _, err := os.Stat(storage.IntermediateDir(tc.cfg.OutputDir)); !os.IsNotExist(err) {
				t.Errorf("rejected config left %s behind", storage.IntermediateDir(tc.cfg.OutputDir))
			}
		})
	}
}

func TestDuplicateInputsBecomeOneTask(t *testing.T) {
	t.Parallel()
	in := inputs(t, 2)
	c := newTestCoordinator(t, Config{Inputs: []string{in[0], in[1], in[0], filepath.Dir(in[1]) + "/./" + filepath.Base(in[1])}})
	if got := c.Stats().MapTasks; got != 2 {
		t.Errorf("MapTasks = %d, want 2", got)
	}
}

// The original implementation reported the job done as soon as the map tasks
// finished, without ever running a reduce task.
func TestDoneOnlyAfterEveryReduceTaskCommits(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator(t, Config{Inputs: inputs(t, 2), NReduce: 2})
	m0 := mustRun(t, c, "w", protocol.Map)
	m1 := mustRun(t, c, "w", protocol.Map)
	report(t, c, "w", m0, "")
	report(t, c, "w", m1, "")
	if isDone(c) {
		t.Fatal("done after the map phase")
	}

	r0 := mustRun(t, c, "w", protocol.Reduce)
	if len(r0.MapOutputs) != 2 || r0.MapOutputs[1] != storage.MapAttemptDir(c.cfg.OutputDir, m1.ID, m1.Attempt) {
		t.Errorf("reduce task reads %v, want the accepted map attempt directories", r0.MapOutputs)
	}
	r1 := mustRun(t, c, "w", protocol.Reduce)
	report(t, c, "w", r0, "")
	if isDone(c) {
		t.Fatal("done with a reduce task outstanding")
	}
	report(t, c, "w", r1, "")
	if !isDone(c) || c.Err() != nil {
		t.Fatalf("done=%v err=%v after all reduce tasks", isDone(c), c.Err())
	}
	if got := request(t, c, "w").Action; got != protocol.Exit {
		t.Errorf("action after completion = %v, want Exit", got)
	}
	if _, err := os.Stat(storage.IntermediateDir(c.cfg.OutputDir)); !os.IsNotExist(err) {
		t.Error("intermediate directory not cleaned up")
	}
}

func TestReduceWaitsForMapPhase(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator(t, Config{Inputs: inputs(t, 1), BackupAfter: -1})
	m := mustRun(t, c, "a", protocol.Map)
	start := time.Now()
	if got := request(t, c, "b").Action; got != protocol.Wait {
		t.Fatalf("action while the only map task runs = %v, want Wait", got)
	}
	if waited := time.Since(start); waited < longPoll/2 {
		t.Errorf("Wait returned after %v; RequestTask should long-poll", waited)
	}

	// A waiting worker is woken as soon as the map completes.
	got := make(chan protocol.RequestTaskReply)
	go func() { got <- request(t, c, "b") }()
	time.Sleep(20 * time.Millisecond)
	report(t, c, "a", m, "")
	select {
	case reply := <-got:
		if reply.Action != protocol.Run || reply.Task.Kind != protocol.Reduce {
			t.Errorf("woken with %+v, want a reduce task", reply)
		}
	case <-time.After(longPoll / 2):
		t.Error("waiting worker was not woken by the state change")
	}
}

func TestExpiredLeaseIsReassigned(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator(t, Config{Inputs: inputs(t, 1), LeaseTimeout: 50 * time.Millisecond, BackupAfter: -1})
	first := mustRun(t, c, "dead", protocol.Map)
	time.Sleep(80 * time.Millisecond)
	second := mustRun(t, c, "alive", protocol.Map)
	if second.ID != first.ID || second.Attempt == first.Attempt {
		t.Fatalf("reassigned %+v after %+v; want the same task as a new attempt", second, first)
	}
	if got := c.Stats().LeaseExpirations; got != 1 {
		t.Errorf("LeaseExpirations = %d, want 1", got)
	}
}

func TestRenewingKeepsLease(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator(t, Config{Inputs: inputs(t, 1), LeaseTimeout: 60 * time.Millisecond, BackupAfter: -1})
	task := mustRun(t, c, "a", protocol.Map)
	for range 5 {
		time.Sleep(25 * time.Millisecond)
		if !renew(t, c, "a", task) {
			t.Fatal("renewal refused")
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expireLeases(time.Now())
	if got := c.assign("b", time.Now()); got != nil {
		t.Errorf("renewed task was reassigned: %+v", got)
	}
}

func TestLateRenewalOfExpiredAttemptIsRefused(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator(t, Config{Inputs: inputs(t, 1), LeaseTimeout: 30 * time.Millisecond, BackupAfter: -1})
	task := mustRun(t, c, "slow", protocol.Map)
	time.Sleep(50 * time.Millisecond)
	c.mu.Lock()
	c.expireLeases(time.Now())
	c.mu.Unlock()
	if renew(t, c, "slow", task) {
		t.Fatal("expired attempt told to continue")
	}
	retry := mustRun(t, c, "fresh", protocol.Map)
	if retry.ID != task.ID || retry.Attempt == task.Attempt {
		t.Fatalf("got %+v after the expiry, want a new attempt of task %d", retry, task.ID)
	}
	if !report(t, c, "slow", task, "") {
		t.Error("success from the expired attempt not accepted while the task was still open")
	}
	if renew(t, c, "fresh", retry) {
		t.Error("retry told to continue after the expired attempt completed the task")
	}
}

// A worker's renewal can reach the coordinator after the error report that
// ended the same attempt. The renewal must not bring the attempt back.
func TestStaleRenewalDoesNotResurrectFailedAttempt(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator(t, Config{Inputs: inputs(t, 1), LeaseTimeout: time.Minute, BackupAfter: -1})
	failed := mustRun(t, c, "w", protocol.Map)
	report(t, c, "w", failed, "disk on fire")
	if renew(t, c, "w", failed) {
		t.Fatal("failed attempt told to continue")
	}
	c.mu.Lock()
	live := len(c.maps[failed.ID].attempts)
	c.mu.Unlock()
	if live != 0 {
		t.Fatalf("task has %d live attempts after the stale renewal, want 0", live)
	}
	start := time.Now()
	retry := mustRun(t, c, "next", protocol.Map)
	if retry.ID != failed.ID || retry.Attempt == failed.Attempt {
		t.Fatalf("got %+v, want a new attempt of task %d", retry, failed.ID)
	}
	if waited := time.Since(start); waited > longPoll/2 {
		t.Errorf("reassignment took %v; want it at once, not after the lease timeout", waited)
	}
}

func TestBackupAttemptForSlowTask(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator(t, Config{Inputs: inputs(t, 1), LeaseTimeout: time.Minute, BackupAfter: 40 * time.Millisecond})
	slow := mustRun(t, c, "slow", protocol.Map)
	if got := request(t, c, "slow").Action; got != protocol.Wait {
		t.Fatalf("worker got a backup of its own task (action %v)", got)
	}
	backup := mustRun(t, c, "fast", protocol.Map)
	if backup.ID != slow.ID || backup.Attempt == slow.Attempt {
		t.Fatalf("backup = %+v, want a new attempt of task %d", backup, slow.ID)
	}
	if got := request(t, c, "third").Action; got != protocol.Wait {
		t.Errorf("a task got a second backup (action %v)", got)
	}

	if !report(t, c, "fast", backup, "") {
		t.Fatal("first success not accepted")
	}
	if renew(t, c, "slow", slow) {
		t.Error("slow attempt told to continue after the task completed")
	}
	if report(t, c, "slow", slow, "") {
		t.Error("second success accepted")
	}
	st := c.Stats()
	if st.Backups != 1 || st.Duplicates != 1 {
		t.Errorf("Backups=%d Duplicates=%d, want 1 and 1", st.Backups, st.Duplicates)
	}
}

func TestNoBackupWhileIdleTasksRemain(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator(t, Config{Inputs: inputs(t, 2), LeaseTimeout: time.Minute, BackupAfter: time.Millisecond})
	first := mustRun(t, c, "a", protocol.Map)
	time.Sleep(5 * time.Millisecond)
	next := mustRun(t, c, "b", protocol.Map)
	if next.ID == first.ID {
		t.Error("backup started while an unassigned task was waiting")
	}
}

func TestFailedAttemptIsRetriedThenFailsJob(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator(t, Config{Inputs: inputs(t, 1), MaxAttempts: 2})
	a := mustRun(t, c, "w", protocol.Map)
	report(t, c, "w", a, "disk on fire")
	b := mustRun(t, c, "w", protocol.Map)
	if b.ID != a.ID {
		t.Fatalf("failed task not retried: got %+v", b)
	}
	if isDone(c) {
		t.Fatal("job ended before MaxAttempts")
	}
	report(t, c, "w", b, "disk still on fire")
	if !isDone(c) || c.Err() == nil || !strings.Contains(c.Err().Error(), "disk still on fire") {
		t.Fatalf("done=%v err=%v; want the job failed with the last error", isDone(c), c.Err())
	}
	if got := request(t, c, "w").Action; got != protocol.Exit {
		t.Errorf("action after failure = %v, want Exit", got)
	}
}

func TestRejectsBadRequests(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator(t, Config{})
	var reply protocol.RequestTaskReply
	if err := c.requestTask(&protocol.RequestTaskArgs{Version: protocol.Version + 1, WorkerID: "w"}, &reply); err == nil {
		t.Error("accepted a different protocol version")
	}
	if err := c.requestTask(&protocol.RequestTaskArgs{Version: protocol.Version}, &reply); err == nil {
		t.Error("accepted an empty worker ID")
	}
	var rr protocol.ReportTaskReply
	if err := c.reportTask(&protocol.ReportTaskArgs{TaskRef: protocol.TaskRef{WorkerID: "w", Kind: protocol.Reduce, ID: 99}}, &rr); err == nil {
		t.Error("accepted a report for a task that does not exist")
	}
}

// The original coordinator crashed on startup every time. Serve and Shutdown
// must work with and without connected workers.
func TestServeAndShutdown(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator(t, Config{})
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- c.Serve(l) }()
	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c.Shutdown(time.Second)
	if err := <-served; err != nil {
		t.Errorf("Serve returned %v after Shutdown", err)
	}
	if err := c.Serve(l); err != ErrClosed {
		t.Errorf("Serve after Shutdown = %v, want ErrClosed", err)
	}
}

func TestShutdownWaitsForActiveWorkersToExit(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator(t, Config{Inputs: inputs(t, 1), NReduce: 1})
	m := mustRun(t, c, "a", protocol.Map)
	request(t, c, "b") // b is idle but active
	report(t, c, "a", m, "")
	r := mustRun(t, c, "a", protocol.Reduce)
	report(t, c, "a", r, "")

	go func() {
		time.Sleep(100 * time.Millisecond)
		request(t, c, "a")
		request(t, c, "b")
	}()
	start := time.Now()
	c.Shutdown(5 * time.Second)
	if waited := time.Since(start); waited < 100*time.Millisecond || waited > 2*time.Second {
		t.Errorf("Shutdown took %v; want it to return once both workers were told to exit", waited)
	}
}

func TestShutdownEndsLongPolls(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator(t, Config{Inputs: inputs(t, 1), BackupAfter: -1})
	mustRun(t, c, "a", protocol.Map)
	polled := make(chan error)
	go func() {
		var reply protocol.RequestTaskReply
		polled <- c.requestTask(&protocol.RequestTaskArgs{Version: protocol.Version, WorkerID: "b"}, &reply)
	}()
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	c.Shutdown(0)
	if err := <-polled; err != ErrClosed {
		t.Errorf("long poll returned %v, want ErrClosed", err)
	}
	if waited := time.Since(start); waited > longPoll/2 {
		t.Errorf("long poll outlived Shutdown by %v", waited)
	}
}
