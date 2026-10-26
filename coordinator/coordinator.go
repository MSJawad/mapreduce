// Package coordinator schedules a MapReduce job across workers.
//
// The coordinator owns all job state. It hands out map tasks (one per input
// file) and, once every map task has completed, reduce tasks (one per output
// partition). Each assignment is an attempt that holds a lease the worker must
// keep renewing. An attempt whose lease expires is presumed dead and its task
// becomes available again; near the end of a phase, tasks that have run for
// too long get a backup attempt on another worker. The first attempt to report
// success wins and later reports are ignored.
package coordinator

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/MSJawad/mapreduce/internal/protocol"
	"github.com/MSJawad/mapreduce/internal/storage"
	"github.com/MSJawad/mapreduce/job"
)

// Defaults for Config fields left at zero.
const (
	DefaultLeaseTimeout = 10 * time.Second
	DefaultBackupAfter  = 10 * time.Second
	DefaultMaxAttempts  = 4
)

// How long a RequestTask call waits for work before telling the worker to ask
// again, and how often it rechecks leases and backups meanwhile.
const (
	longPoll  = time.Second
	pollEvery = 50 * time.Millisecond
)

// ErrClosed is returned by Serve after Shutdown.
var ErrClosed = errors.New("coordinator: closed")

// Config describes a job and how to run it.
type Config struct {
	// Job is the registered job name and Params its parameters.
	Job    string
	Params map[string]string

	// Inputs are the files to process, one map task each. Every worker must
	// be able to read them at the same paths.
	Inputs []string

	// NReduce is the number of reduce tasks, and so of output files.
	NReduce int

	// OutputDir receives the output files part-00000 .. part-NNNNN. It must
	// be empty or not exist yet, and be shared with every worker: workers
	// also exchange intermediate data through it.
	OutputDir string

	// LeaseTimeout is how long an attempt may go without renewing its lease
	// before its task is handed to another worker. Zero means
	// DefaultLeaseTimeout.
	LeaseTimeout time.Duration

	// BackupAfter is how long a task must have been running, once its phase
	// has no unassigned tasks left, before a backup attempt is started on
	// another worker. Zero means DefaultBackupAfter; negative disables
	// backup tasks.
	BackupAfter time.Duration

	// MaxAttempts is how many failed attempts (reported errors, not lease
	// expiries) a task may have before the whole job fails. Zero means
	// DefaultMaxAttempts.
	MaxAttempts int

	// KeepIntermediate leaves intermediate files in place after success.
	KeepIntermediate bool

	// Logger receives progress and failure events. Nil discards them.
	Logger *slog.Logger
}

// Stats counts what happened while running the job.
type Stats struct {
	MapTasks         int
	ReduceTasks      int
	Workers          int // distinct workers that asked for work
	Attempts         int // attempts started, including backups
	Backups          int // backup attempts started for slow tasks
	LeaseExpirations int // attempts presumed dead
	Failures         int // attempts that reported an error
	Duplicates       int // success reports ignored because the task was done
	Elapsed          time.Duration
}

type taskState int

const (
	idle taskState = iota
	running
	done
)

type attempt struct {
	worker     string
	started    time.Time
	leaseUntil time.Time
}

type task struct {
	kind     protocol.Kind
	id       int
	input    string // map tasks only
	state    taskState
	attempts map[int]*attempt // live attempts by attempt number
	failures int
	winner   int // accepted attempt, once done
}

type workerInfo struct {
	lastSeen time.Time
	exitSent bool
}

// Coordinator runs one job. Create it with New, serve workers with Serve,
// wait on Done and finish with Shutdown.
type Coordinator struct {
	cfg     Config
	log     *slog.Logger
	started time.Time
	server  *rpc.Server

	mu          sync.Mutex
	maps        []*task
	reduces     []*task
	mapsLeft    int
	reducesLeft int
	nextAttempt int
	workers     map[string]*workerInfo
	stats       Stats
	err         error
	finished    bool
	done        chan struct{}
	changed     chan struct{} // closed and replaced whenever state changes
	stopping    chan struct{} // closed by Shutdown to end long polls

	connMu    sync.Mutex
	closed    bool
	listeners map[net.Listener]struct{}
	conns     map[net.Conn]struct{}
	connWG    sync.WaitGroup
}

// New validates cfg, prepares the output directory and returns a coordinator
// ready to serve.
func New(cfg Config) (*Coordinator, error) {
	if _, err := job.New(cfg.Job, cfg.Params); err != nil {
		return nil, err
	}
	if cfg.NReduce < 1 {
		return nil, errors.New("NReduce must be at least 1")
	}
	if cfg.OutputDir == "" {
		return nil, errors.New("no output directory")
	}
	inputs := dedupe(cfg.Inputs)
	if len(inputs) == 0 {
		return nil, errors.New("no input files")
	}
	for _, in := range inputs {
		if st, err := os.Stat(in); err != nil {
			return nil, err
		} else if st.IsDir() {
			return nil, fmt.Errorf("input %s is a directory", in)
		}
	}
	if err := prepareOutputDir(cfg.OutputDir); err != nil {
		return nil, err
	}
	if cfg.LeaseTimeout == 0 {
		cfg.LeaseTimeout = DefaultLeaseTimeout
	}
	if cfg.BackupAfter == 0 {
		cfg.BackupAfter = DefaultBackupAfter
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = DefaultMaxAttempts
	}
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	c := &Coordinator{
		cfg:         cfg,
		log:         log,
		started:     time.Now(),
		server:      rpc.NewServer(),
		mapsLeft:    len(inputs),
		reducesLeft: cfg.NReduce,
		workers:     map[string]*workerInfo{},
		done:        make(chan struct{}),
		changed:     make(chan struct{}),
		stopping:    make(chan struct{}),
		listeners:   map[net.Listener]struct{}{},
		conns:       map[net.Conn]struct{}{},
	}
	for i, in := range inputs {
		c.maps = append(c.maps, &task{kind: protocol.Map, id: i, input: in, attempts: map[int]*attempt{}})
	}
	for r := range cfg.NReduce {
		c.reduces = append(c.reduces, &task{kind: protocol.Reduce, id: r, attempts: map[int]*attempt{}})
	}
	c.stats.MapTasks = len(c.maps)
	c.stats.ReduceTasks = len(c.reduces)
	if err := c.server.RegisterName(protocol.ServiceName, &service{c}); err != nil {
		return nil, err
	}
	log.Info("job ready", "job", cfg.Job, "map_tasks", len(c.maps), "reduce_tasks", cfg.NReduce, "output", cfg.OutputDir)
	return c, nil
}

func dedupe(paths []string) []string {
	var out []string
	seen := map[string]bool{}
	for _, p := range paths {
		clean := filepath.Clean(p)
		if !seen[clean] {
			seen[clean] = true
			out = append(out, p)
		}
	}
	return out
}

func prepareOutputDir(dir string) error {
	entries, err := os.ReadDir(dir)
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return err
	case len(entries) > 0:
		return fmt.Errorf("output directory %s is not empty", dir)
	}
	for _, d := range []string{dir, storage.IntermediateDir(dir), storage.TempDir(dir)} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	return nil
}

// Done is closed when the job has finished, successfully or not.
func (c *Coordinator) Done() <-chan struct{} { return c.done }

// Err returns why the job failed, or nil if it succeeded or is still running.
func (c *Coordinator) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// Stats returns a snapshot of the job's counters.
func (c *Coordinator) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.stats
	s.Workers = len(c.workers)
	return s
}

// Serve accepts worker connections on l until Shutdown is called.
func (c *Coordinator) Serve(l net.Listener) error {
	c.connMu.Lock()
	if c.closed {
		c.connMu.Unlock()
		l.Close()
		return ErrClosed
	}
	c.listeners[l] = struct{}{}
	c.connMu.Unlock()

	for {
		conn, err := l.Accept()
		if err != nil {
			c.connMu.Lock()
			closed := c.closed
			c.connMu.Unlock()
			if closed {
				return nil
			}
			return err
		}
		c.connMu.Lock()
		if c.closed {
			c.connMu.Unlock()
			conn.Close()
			return nil
		}
		c.conns[conn] = struct{}{}
		c.connWG.Add(1)
		c.connMu.Unlock()
		go func() {
			defer c.connWG.Done()
			c.server.ServeConn(conn)
			c.connMu.Lock()
			delete(c.conns, conn)
			c.connMu.Unlock()
		}()
	}
}

// Shutdown stops the coordinator. If the job has finished, it first gives
// recently active workers until grace elapses to collect their exit
// instruction, so they stop cleanly instead of finding the coordinator gone.
func (c *Coordinator) Shutdown(grace time.Duration) {
	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) && c.workersAwaitingExit() > 0 {
		time.Sleep(pollEvery)
	}
	c.connMu.Lock()
	if !c.closed {
		close(c.stopping)
	}
	c.closed = true
	for l := range c.listeners {
		l.Close()
	}
	for conn := range c.conns {
		conn.Close()
	}
	c.connMu.Unlock()
	c.connWG.Wait()
}

func (c *Coordinator) workersAwaitingExit() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.finished {
		return 0
	}
	n := 0
	for _, w := range c.workers {
		if !w.exitSent && time.Since(w.lastSeen) < c.cfg.LeaseTimeout {
			n++
		}
	}
	return n
}

// service exposes the RPC methods. It is a separate type so that net/rpc
// only sees methods with the RPC signature.
type service struct{ c *Coordinator }

func (s *service) RequestTask(args *protocol.RequestTaskArgs, reply *protocol.RequestTaskReply) error {
	return s.c.requestTask(args, reply)
}

func (s *service) RenewLease(args *protocol.RenewLeaseArgs, reply *protocol.RenewLeaseReply) error {
	return s.c.renewLease(args, reply)
}

func (s *service) ReportTask(args *protocol.ReportTaskArgs, reply *protocol.ReportTaskReply) error {
	return s.c.reportTask(args, reply)
}

func (c *Coordinator) requestTask(args *protocol.RequestTaskArgs, reply *protocol.RequestTaskReply) error {
	if args.Version != protocol.Version {
		return fmt.Errorf("protocol version %d not supported (coordinator speaks %d)", args.Version, protocol.Version)
	}
	if args.WorkerID == "" {
		return errors.New("missing worker ID")
	}
	deadline := time.Now().Add(longPoll)
	for {
		c.mu.Lock()
		now := time.Now()
		c.touch(args.WorkerID, now)
		if c.finished {
			c.workers[args.WorkerID].exitSent = true
			c.mu.Unlock()
			reply.Action = protocol.Exit
			return nil
		}
		c.expireLeases(now)
		if t := c.assign(args.WorkerID, now); t != nil {
			reply.Action = protocol.Run
			reply.Task = *t
			c.mu.Unlock()
			return nil
		}
		changed := c.changed
		c.mu.Unlock()

		wait := min(pollEvery, time.Until(deadline))
		if wait <= 0 {
			reply.Action = protocol.Wait
			return nil
		}
		select {
		case <-changed:
		case <-time.After(wait):
		case <-c.stopping:
			return ErrClosed
		}
	}
}

func (c *Coordinator) touch(worker string, now time.Time) {
	w, ok := c.workers[worker]
	if !ok {
		w = &workerInfo{}
		c.workers[worker] = w
		c.log.Info("worker joined", "worker", worker)
	}
	w.lastSeen = now
}

// expireLeases drops attempts whose lease ran out and makes their tasks
// available again when no live attempt is left.
func (c *Coordinator) expireLeases(now time.Time) {
	for _, t := range c.phase() {
		if t.state != running {
			continue
		}
		for n, a := range t.attempts {
			if now.After(a.leaseUntil) {
				delete(t.attempts, n)
				c.stats.LeaseExpirations++
				c.log.Warn("lease expired; task will be re-executed", "kind", t.kind, "task", t.id, "attempt", n, "worker", a.worker)
			}
		}
		if len(t.attempts) == 0 {
			t.state = idle
		}
	}
}

// phase returns the tasks currently being scheduled: map tasks until all of
// them are done, then reduce tasks.
func (c *Coordinator) phase() []*task {
	if c.mapsLeft > 0 {
		return c.maps
	}
	return c.reduces
}

// assign picks a task for worker: an idle task if there is one, otherwise a
// backup attempt for the longest-running task in the phase.
func (c *Coordinator) assign(worker string, now time.Time) *protocol.Task {
	tasks := c.phase()
	for _, t := range tasks {
		if t.state == idle {
			return c.start(t, worker, now, false)
		}
	}
	if c.cfg.BackupAfter < 0 {
		return nil
	}
	var slowest *task
	var slowestStart time.Time
	for _, t := range tasks {
		if t.state != running || len(t.attempts) != 1 {
			continue
		}
		for _, a := range t.attempts {
			if a.worker != worker && now.Sub(a.started) >= c.cfg.BackupAfter &&
				(slowest == nil || a.started.Before(slowestStart)) {
				slowest, slowestStart = t, a.started
			}
		}
	}
	if slowest == nil {
		return nil
	}
	return c.start(slowest, worker, now, true)
}

func (c *Coordinator) start(t *task, worker string, now time.Time, backup bool) *protocol.Task {
	c.nextAttempt++
	n := c.nextAttempt
	t.attempts[n] = &attempt{worker: worker, started: now, leaseUntil: now.Add(c.cfg.LeaseTimeout)}
	t.state = running
	c.stats.Attempts++
	if backup {
		c.stats.Backups++
		c.log.Info("starting backup attempt for slow task", "kind", t.kind, "task", t.id, "attempt", n, "worker", worker)
	} else {
		c.log.Debug("task assigned", "kind", t.kind, "task", t.id, "attempt", n, "worker", worker)
	}
	pt := &protocol.Task{
		Kind:         t.kind,
		ID:           t.id,
		Attempt:      n,
		Job:          c.cfg.Job,
		Params:       c.cfg.Params,
		OutputDir:    c.cfg.OutputDir,
		NReduce:      c.cfg.NReduce,
		LeaseTimeout: c.cfg.LeaseTimeout,
	}
	if t.kind == protocol.Map {
		pt.Input = t.input
	} else {
		for _, m := range c.maps {
			pt.MapOutputs = append(pt.MapOutputs, storage.MapAttemptDir(c.cfg.OutputDir, m.id, m.winner))
		}
	}
	return pt
}

func (c *Coordinator) lookup(kind protocol.Kind, id int) (*task, error) {
	var tasks []*task
	switch kind {
	case protocol.Map:
		tasks = c.maps
	case protocol.Reduce:
		tasks = c.reduces
	}
	if id < 0 || id >= len(tasks) {
		return nil, fmt.Errorf("no %v task %d", kind, id)
	}
	return tasks[id], nil
}

func (c *Coordinator) renewLease(args *protocol.RenewLeaseArgs, reply *protocol.RenewLeaseReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, err := c.lookup(args.Kind, args.ID)
	if err != nil {
		return err
	}
	now := time.Now()
	c.touch(args.WorkerID, now)
	if c.finished || t.state == done {
		return nil
	}
	a, ok := t.attempts[args.Attempt]
	if !ok {
		// The lease had expired, but the worker is evidently still alive
		// (a network hiccup or a long pause). Its result is as good as any
		// other attempt's, so let it carry on alongside the reassignment.
		a = &attempt{worker: args.WorkerID, started: now}
		t.attempts[args.Attempt] = a
		t.state = running
		c.log.Info("expired attempt resumed renewing", "kind", t.kind, "task", t.id, "attempt", args.Attempt, "worker", args.WorkerID)
	}
	a.leaseUntil = now.Add(c.cfg.LeaseTimeout)
	reply.Continue = true
	return nil
}

func (c *Coordinator) reportTask(args *protocol.ReportTaskArgs, reply *protocol.ReportTaskReply) error {
	c.mu.Lock()
	t, err := c.lookup(args.Kind, args.ID)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	c.touch(args.WorkerID, time.Now())
	if c.finished || t.state == done {
		if args.Error == "" {
			c.stats.Duplicates++
		}
		c.mu.Unlock()
		return nil
	}
	reply.Accepted = true
	defer c.notify()

	if args.Error != "" {
		delete(t.attempts, args.Attempt)
		t.failures++
		c.stats.Failures++
		c.log.Warn("task attempt failed", "kind", t.kind, "task", t.id, "attempt", args.Attempt, "worker", args.WorkerID, "error", args.Error)
		if t.failures >= c.cfg.MaxAttempts {
			c.finish(fmt.Errorf("%v task %d failed %d times, last error: %s", t.kind, t.id, t.failures, args.Error))
		} else if len(t.attempts) == 0 {
			t.state = idle
		}
		c.mu.Unlock()
		return nil
	}

	t.state = done
	t.winner = args.Attempt
	t.attempts = map[int]*attempt{}
	c.log.Debug("task done", "kind", t.kind, "task", t.id, "attempt", args.Attempt, "worker", args.WorkerID)
	if t.kind == protocol.Map {
		c.mapsLeft--
		if c.mapsLeft == 0 {
			c.log.Info("map phase complete; starting reduce phase", "elapsed", time.Since(c.started).Round(time.Millisecond))
		}
		c.mu.Unlock()
		return nil
	}
	c.reducesLeft--
	if c.reducesLeft > 0 {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	// Every partition is committed. Clean up outside the lock, then mark the
	// job finished.
	var cleanupErr error
	if !c.cfg.KeepIntermediate {
		cleanupErr = errors.Join(os.RemoveAll(storage.IntermediateDir(c.cfg.OutputDir)), os.RemoveAll(storage.TempDir(c.cfg.OutputDir)))
	}
	c.mu.Lock()
	if cleanupErr != nil {
		c.log.Warn("could not remove intermediate files", "error", cleanupErr)
	}
	c.finish(nil)
	c.mu.Unlock()
	return nil
}

// finish marks the job over. c.mu must be held.
func (c *Coordinator) finish(err error) {
	if c.finished {
		return
	}
	c.finished = true
	c.err = err
	c.stats.Elapsed = time.Since(c.started)
	if err != nil {
		c.log.Error("job failed", "error", err)
	} else {
		c.log.Info("job complete", "elapsed", c.stats.Elapsed.Round(time.Millisecond), "outputs", c.cfg.NReduce)
	}
	close(c.done)
}

// notify wakes RequestTask calls waiting for a state change.
func (c *Coordinator) notify() {
	c.mu.Lock()
	close(c.changed)
	c.changed = make(chan struct{})
	c.mu.Unlock()
}

// OutputFiles lists the job's output files.
func (c *Coordinator) OutputFiles() []string {
	files := make([]string, c.cfg.NReduce)
	for r := range files {
		files[r] = storage.OutputFile(c.cfg.OutputDir, r)
	}
	return files
}

// String summarizes the counters for logs.
func (s Stats) String() string {
	return fmt.Sprintf("map_tasks=%d reduce_tasks=%d workers=%d attempts=%d backups=%d lease_expirations=%d failures=%d elapsed=%s",
		s.MapTasks, s.ReduceTasks, s.Workers, s.Attempts, s.Backups, s.LeaseExpirations, s.Failures, s.Elapsed.Round(time.Millisecond))
}
