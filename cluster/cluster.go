// Package cluster runs a coordinator and a set of workers inside one process,
// talking to each other over real TCP connections on the loopback interface.
// It backs the "local" CLI command, the integration and fault-injection
// tests, and the benchmarks.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/MSJawad/mapreduce/coordinator"
	"github.com/MSJawad/mapreduce/worker"
)

// Options configures an in-process run.
type Options struct {
	// Job is the coordinator configuration; Job.Logger is also used for the
	// workers.
	Job coordinator.Config

	// Workers is how many workers to start.
	Workers int

	// WorkerHooks, if set, returns the fault-injection hooks for worker i.
	// Calling kill stops that worker abruptly, as if its process died: it
	// stops renewing its lease and reports nothing for the task in hand.
	WorkerHooks func(i int, kill func()) worker.Hooks

	// WorkerTimeout is each worker's CoordinatorTimeout. Zero means the
	// worker default.
	WorkerTimeout time.Duration
}

// Result reports how a run went.
type Result struct {
	Stats coordinator.Stats

	// WorkerErrors holds what each worker's Run returned. Workers stopped
	// because the job finished return nil; workers killed or crashed by a
	// test return the corresponding error.
	WorkerErrors []error
}

// WorkerID names worker i, so tests can recognise it in TaskInfo.
func WorkerID(i int) string { return fmt.Sprintf("worker-%d", i) }

// Run executes the job and waits for it to finish or for ctx to be done.
func Run(ctx context.Context, opts Options) (Result, error) {
	if opts.Workers < 1 {
		return Result{}, errors.New("need at least one worker")
	}
	c, err := coordinator.New(opts.Job)
	if err != nil {
		return Result{}, err
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return Result{}, err
	}
	served := make(chan error, 1)
	go func() { served <- c.Serve(l) }()

	log := opts.Job.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	workersCtx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()
	res := Result{WorkerErrors: make([]error, opts.Workers)}
	var wg sync.WaitGroup
	for i := range opts.Workers {
		workerCtx, kill := context.WithCancel(workersCtx)
		defer kill()
		cfg := worker.Config{
			Coordinator:        l.Addr().String(),
			ID:                 WorkerID(i),
			CoordinatorTimeout: opts.WorkerTimeout,
			Logger:             log,
		}
		if opts.WorkerHooks != nil {
			cfg.Hooks = opts.WorkerHooks(i, kill)
		}
		wg.Go(func() { res.WorkerErrors[i] = worker.Run(workerCtx, cfg) })
	}

	select {
	case <-c.Done():
		err = c.Err()
	case <-ctx.Done():
		err = ctx.Err()
	}
	c.Shutdown(2 * time.Second)
	stopWorkers()
	wg.Wait()
	if serveErr := <-served; serveErr != nil && err == nil {
		err = serveErr
	}
	res.Stats = c.Stats()
	return res, err
}
