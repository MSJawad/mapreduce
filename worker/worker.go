// Package worker executes map and reduce tasks handed out by a coordinator.
//
// A worker loops: ask the coordinator for a task, run it while renewing its
// lease in the background, report the result, repeat until told to exit.
// Every attempt writes to files private to that attempt and only makes them
// visible atomically, so a worker can die at any point without leaving
// anything a later attempt or a reducer would mistake for valid output.
package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/MSJawad/mapreduce/internal/protocol"
	"github.com/MSJawad/mapreduce/internal/storage"
	"github.com/MSJawad/mapreduce/job"
)

// DefaultCoordinatorTimeout is how long a worker keeps retrying an
// unreachable coordinator before giving up.
const DefaultCoordinatorTimeout = 30 * time.Second

// ErrCoordinatorUnavailable is returned by Run when the coordinator could not
// be reached for longer than the configured timeout.
var ErrCoordinatorUnavailable = errors.New("coordinator unavailable")

// ErrCrashed is returned by Run when a fault-injection hook simulated a crash.
var ErrCrashed = errors.New("worker crashed (injected fault)")

// Config configures a worker.
type Config struct {
	// Coordinator is the coordinator's TCP address, host:port.
	Coordinator string

	// ID names the worker in coordinator logs. Empty means
	// hostname-pid-random.
	ID string

	// CoordinatorTimeout bounds how long the worker retries an unreachable
	// coordinator, including at startup. Zero means
	// DefaultCoordinatorTimeout; negative is an error.
	CoordinatorTimeout time.Duration

	// Logger receives task events. Nil discards them.
	Logger *slog.Logger

	// Hooks inject faults for testing. Leave zero in production.
	Hooks Hooks
}

// TaskInfo identifies the task a hook is called for.
type TaskInfo struct {
	Worker  string
	Kind    string // "map" or "reduce"
	ID      int
	Attempt int
}

// Hooks are fault-injection points used by tests. Every field is optional.
type Hooks struct {
	// TaskStarted runs at the start of every task, while the lease is
	// already being renewed. Blocking in it models a slow worker; ctx is
	// cancelled if the attempt becomes useless or the worker is stopped.
	TaskStarted func(ctx context.Context, t TaskInfo)

	// PartialOutput runs once part of a task's output is on disk but before
	// the task is committed: after the first map partition is written, or
	// after the first reduce output record. Returning an error simulates a
	// crash at that point: Run stops at once with ErrCrashed, leaving its
	// files behind and reporting nothing, as if the process had died.
	PartialOutput func(t TaskInfo) error
}

type worker struct {
	cfg         Config
	log         *slog.Logger
	client      *client
	lastContact atomic.Int64 // unix nanoseconds
}

// crashError carries an injected crash up to Run.
type crashError struct{ err error }

func (e *crashError) Error() string { return e.err.Error() }

// Run executes tasks until the coordinator says the job is over (returning
// nil), ctx is cancelled (returning ctx.Err() without reporting the task in
// progress, like a killed process), or the coordinator stays unreachable.
func Run(ctx context.Context, cfg Config) error {
	if cfg.Coordinator == "" {
		return errors.New("no coordinator address")
	}
	if cfg.ID == "" {
		cfg.ID = defaultID()
	}
	if cfg.CoordinatorTimeout == 0 {
		cfg.CoordinatorTimeout = DefaultCoordinatorTimeout
	}
	if cfg.CoordinatorTimeout < 0 {
		return fmt.Errorf("CoordinatorTimeout must not be negative, got %v", cfg.CoordinatorTimeout)
	}
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	w := &worker{cfg: cfg, log: log.With("worker", cfg.ID), client: &client{addr: cfg.Coordinator}}
	defer w.client.close()
	w.contact()
	w.log.Info("worker started", "coordinator", cfg.Coordinator)

	for {
		args := protocol.RequestTaskArgs{Version: protocol.Version, WorkerID: cfg.ID}
		var reply protocol.RequestTaskReply
		if err := w.call(ctx, protocol.MethodRequestTask, &args, &reply); err != nil {
			return err
		}
		switch reply.Action {
		case protocol.Exit:
			w.log.Info("job finished; worker exiting")
			return nil
		case protocol.Wait:
		case protocol.Run:
			if err := w.runTask(ctx, reply.Task); err != nil {
				return err
			}
		default:
			return fmt.Errorf("coordinator sent unknown action %d", reply.Action)
		}
	}
}

func defaultID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "worker"
	}
	var b [2]byte
	rand.Read(b[:])
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(b[:]))
}

func (w *worker) contact() { w.lastContact.Store(time.Now().UnixNano()) }

// call makes an RPC, retrying transport failures with backoff until the
// coordinator has been unreachable for longer than CoordinatorTimeout.
func (w *worker) call(ctx context.Context, method string, args, reply any) error {
	backoff := 50 * time.Millisecond
	for {
		err := w.client.call(ctx, method, args, reply)
		if err == nil {
			w.contact()
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if isServerError(err) {
			return fmt.Errorf("coordinator rejected %s: %w", method, err)
		}
		since := time.Since(time.Unix(0, w.lastContact.Load()))
		if since > w.cfg.CoordinatorTimeout {
			return fmt.Errorf("%w for %s: %v", ErrCoordinatorUnavailable, since.Round(time.Second), err)
		}
		w.log.Debug("coordinator unreachable; retrying", "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, time.Second)
	}
}

func (w *worker) runTask(ctx context.Context, t protocol.Task) error {
	renewEvery := t.LeaseTimeout / 3
	if renewEvery <= 0 {
		return fmt.Errorf("coordinator sent lease timeout %v, too short to renew", t.LeaseTimeout)
	}
	info := TaskInfo{Worker: w.cfg.ID, Kind: t.Kind.String(), ID: t.ID, Attempt: t.Attempt}
	log := w.log.With("kind", t.Kind, "task", t.ID, "attempt", t.Attempt)
	ref := protocol.TaskRef{WorkerID: w.cfg.ID, Kind: t.Kind, ID: t.ID, Attempt: t.Attempt}
	log.Debug("task started")
	began := time.Now()

	taskCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var lost atomic.Bool
	renewed := make(chan struct{})
	go func() {
		defer close(renewed)
		w.renewLease(taskCtx, ref, renewEvery, func() {
			lost.Store(true)
			cancel()
		})
	}()

	if h := w.cfg.Hooks.TaskStarted; h != nil {
		h(taskCtx, info)
	}
	var err error
	if taskCtx.Err() == nil {
		err = w.execute(taskCtx, t, info)
	}
	cancel()
	<-renewed

	var crash *crashError
	switch {
	case ctx.Err() != nil:
		return ctx.Err()
	case errors.As(err, &crash):
		log.Error("worker crashing", "error", crash.err)
		return fmt.Errorf("%w: %w", ErrCrashed, crash.err)
	case lost.Load():
		log.Info("coordinator no longer wants this attempt; abandoning it")
		w.discard(t)
		return nil
	}

	args := protocol.ReportTaskArgs{TaskRef: ref}
	if err != nil {
		w.discard(t)
		args.Error = err.Error()
		log.Warn("task failed", "error", err)
	} else {
		log.Debug("task succeeded", "elapsed", time.Since(began).Round(time.Millisecond))
	}
	var reply protocol.ReportTaskReply
	return w.call(ctx, protocol.MethodReportTask, &args, &reply)
}

// renewLease renews the attempt's lease at the given interval until ctx is
// done. It calls lost if the coordinator says the attempt is no longer
// needed. Failed renewals are only logged: if they keep failing, the lease
// expires and the coordinator reassigns the task, which is the point.
func (w *worker) renewLease(ctx context.Context, ref protocol.TaskRef, every time.Duration, lost func()) {
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		var reply protocol.RenewLeaseReply
		err := w.client.call(ctx, protocol.MethodRenewLease, &protocol.RenewLeaseArgs{TaskRef: ref}, &reply)
		if err != nil {
			if ctx.Err() == nil {
				w.log.Debug("lease renewal failed", "error", err)
			}
			continue
		}
		w.contact()
		if !reply.Continue {
			lost()
			return
		}
	}
}

// discard removes an attempt's private output. Reduce attempts clean up their
// temporary file themselves.
func (w *worker) discard(t protocol.Task) {
	if t.Kind == protocol.Map {
		os.RemoveAll(storage.MapAttemptDir(t.OutputDir, t.ID, t.Attempt))
	}
}

func (w *worker) execute(ctx context.Context, t protocol.Task, info TaskInfo) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	j, err := job.New(t.Job, t.Params)
	if err != nil {
		return err
	}
	switch t.Kind {
	case protocol.Map:
		return w.runMap(ctx, j, t, info)
	case protocol.Reduce:
		return w.runReduce(ctx, j, t, info)
	}
	return fmt.Errorf("unknown task kind %v", t.Kind)
}

func (w *worker) partialOutput(info TaskInfo) error {
	if h := w.cfg.Hooks.PartialOutput; h != nil {
		if err := h(info); err != nil {
			return &crashError{err}
		}
	}
	return nil
}

// runMap applies the mapper to the input file and writes its output, split
// into NReduce partitions sorted by key, into this attempt's private
// directory.
func (w *worker) runMap(ctx context.Context, j *job.Job, t protocol.Task, info TaskInfo) error {
	data, err := os.ReadFile(t.Input)
	if err != nil {
		return err
	}
	buf := newMapBuffer(t.NReduce, j.Combiner)
	if err := j.Mapper.Map(t.Input, data, buf.emit); err != nil {
		return fmt.Errorf("map %s: %w", t.Input, err)
	}
	if buf.err != nil {
		return buf.err
	}

	// Mkdir rather than MkdirAll: if the job has already finished and its
	// intermediate directory is gone, this late attempt must fail instead of
	// recreating it.
	dir := storage.MapAttemptDir(t.OutputDir, t.ID, t.Attempt)
	if err := os.Mkdir(dir, 0o755); err != nil {
		return err
	}
	for r := range t.NReduce {
		if err := ctx.Err(); err != nil {
			return err
		}
		kvs, err := buf.records(r)
		if err != nil {
			return err
		}
		if err := storage.WriteRecords(storage.PartitionFile(dir, r), kvs); err != nil {
			return err
		}
		if r == 0 {
			if err := w.partialOutput(info); err != nil {
				return err
			}
		}
	}
	return nil
}

// runReduce merges this partition's sorted runs from every map task, applies
// the reducer to each key and commits the output file.
func (w *worker) runReduce(ctx context.Context, j *job.Job, t protocol.Task, info TaskInfo) (err error) {
	var readers []*storage.RecordReader
	defer func() {
		for _, rr := range readers {
			rr.Close()
		}
	}()
	for _, dir := range t.MapOutputs {
		rr, err := storage.OpenRecords(storage.PartitionFile(dir, t.ID))
		if err != nil {
			return err
		}
		readers = append(readers, rr)
	}

	out, err := storage.CreateOutput(t.OutputDir, t.ID, t.Attempt)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		var crash *crashError
		switch {
		case committed:
		case errors.As(err, &crash):
			out.Close() // leave the partial file behind, as a real crash would
		default:
			out.Abort()
		}
	}()

	err = storage.MergeGroups(readers, func(key string, values []string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		v, err := j.Reducer.Reduce(key, values)
		if err != nil {
			return fmt.Errorf("reduce %q: %w", key, err)
		}
		if err := out.Write(key, v); err != nil {
			return err
		}
		if out.Records() == 1 {
			return w.partialOutput(info)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if out.Records() == 0 {
		if err := w.partialOutput(info); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := out.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}
