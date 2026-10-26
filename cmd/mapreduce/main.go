// Command mapreduce runs MapReduce jobs: as a coordinator and workers on
// separate machines, as a whole cluster inside one process, or sequentially
// as a reference.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/MSJawad/mapreduce/cluster"
	"github.com/MSJawad/mapreduce/coordinator"
	"github.com/MSJawad/mapreduce/job"
	"github.com/MSJawad/mapreduce/sequential"
	"github.com/MSJawad/mapreduce/worker"
)

const usage = `mapreduce runs MapReduce jobs.

Usage:
  mapreduce coordinator [flags] INPUT...   schedule a job and serve workers over TCP
  mapreduce worker [flags]                 execute tasks for a coordinator
  mapreduce local [flags] INPUT...         run a coordinator and workers in this process
  mapreduce sequential [flags] INPUT...    run a job in one process (reference output)
  mapreduce jobs                           list the available jobs

INPUT arguments may be glob patterns such as 'books/*.txt'.
Run 'mapreduce COMMAND -h' for the flags of a command.
`

// errUsage marks command-line mistakes, which exit with status 2.
var errUsage = errors.New("usage error")

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	switch {
	case err == nil:
	case errors.Is(err, flag.ErrHelp):
	case errors.Is(err, errUsage):
		os.Exit(2)
	default:
		fmt.Fprintln(os.Stderr, "mapreduce:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return errUsage
	}
	cmd, args := args[0], args[1:]
	switch cmd {
	case "coordinator":
		return runCoordinator(ctx, args, stderr)
	case "worker":
		return runWorker(ctx, args, stderr)
	case "local":
		return runLocal(ctx, args, stderr)
	case "sequential":
		return runSequential(args, stderr)
	case "jobs":
		return listJobs(stdout)
	case "help", "-h", "-help", "--help":
		fmt.Fprint(stdout, usage)
		return nil
	}
	fmt.Fprintf(stderr, "unknown command %q\n\n%s", cmd, usage)
	return errUsage
}

// jobFlags are the flags shared by every command that defines a job.
type jobFlags struct {
	job    string
	params paramFlag
	out    string
}

func (j *jobFlags) register(fs *flag.FlagSet, outHelp string) {
	fs.StringVar(&j.job, "job", "wordcount", "job to run (see 'mapreduce jobs')")
	fs.Var(&j.params, "param", "job parameter as key=value; repeatable, empty values are ignored")
	fs.StringVar(&j.out, "out", "", outHelp)
}

// paramFlag collects repeated -param key=value flags.
type paramFlag map[string]string

func (p *paramFlag) String() string { return fmt.Sprint(map[string]string(*p)) }

func (p *paramFlag) Set(s string) error {
	if s == "" {
		return nil
	}
	k, v, ok := strings.Cut(s, "=")
	if !ok || k == "" {
		return fmt.Errorf("want key=value, got %q", s)
	}
	if *p == nil {
		*p = paramFlag{}
	}
	(*p)[k] = v
	return nil
}

// clusterFlags tune scheduling and failure detection.
type clusterFlags struct {
	reduce      int
	lease       time.Duration
	backupAfter time.Duration
	maxAttempts int
	keep        bool
	verbose     bool
}

func (c *clusterFlags) register(fs *flag.FlagSet) {
	fs.IntVar(&c.reduce, "reduce", 8, "number of reduce tasks (output files)")
	fs.DurationVar(&c.lease, "lease", coordinator.DefaultLeaseTimeout, "task lease; a worker silent this long is presumed dead")
	fs.DurationVar(&c.backupAfter, "backup-after", coordinator.DefaultBackupAfter, "start a backup attempt for a task running this long at the end of a phase (negative disables)")
	fs.IntVar(&c.maxAttempts, "max-attempts", coordinator.DefaultMaxAttempts, "failed attempts per task before the job fails")
	fs.BoolVar(&c.keep, "keep-intermediate", false, "keep intermediate files after success")
	fs.BoolVar(&c.verbose, "v", false, "log every task assignment")
}

func (c *clusterFlags) config(j jobFlags, inputs []string, log *slog.Logger) coordinator.Config {
	return coordinator.Config{
		Job:              j.job,
		Params:           j.params,
		Inputs:           inputs,
		NReduce:          c.reduce,
		OutputDir:        j.out,
		LeaseTimeout:     c.lease,
		BackupAfter:      c.backupAfter,
		MaxAttempts:      c.maxAttempts,
		KeepIntermediate: c.keep,
		Logger:           log,
	}
}

func newLogger(w io.Writer, verbose bool) *slog.Logger {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: level}))
}

func newFlagSet(name, synopsis string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage: mapreduce %s\n\nFlags:\n", synopsis)
		fs.PrintDefaults()
	}
	return fs
}

// parse parses flags, mapping errors to errUsage.
func parse(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return errUsage
	}
	return nil
}

func usageErr(fs *flag.FlagSet, msg string) error {
	fmt.Fprintf(fs.Output(), "%s\n\n", msg)
	fs.Usage()
	return errUsage
}

// expandInputs expands glob patterns, so inputs can be given without a shell.
func expandInputs(args []string) ([]string, error) {
	var inputs []string
	for _, a := range args {
		if !strings.ContainsAny(a, "*?[") {
			inputs = append(inputs, a)
			continue
		}
		matches, err := filepath.Glob(a)
		if err != nil {
			return nil, fmt.Errorf("bad pattern %q: %w", a, err)
		}
		if len(matches) == 0 {
			return nil, fmt.Errorf("no files match %q", a)
		}
		inputs = append(inputs, matches...)
	}
	return inputs, nil
}

// jobInputs checks the job flags and returns the input files. Paths are made
// absolute, since workers may run in a different working directory.
func jobInputs(fs *flag.FlagSet, j *jobFlags) ([]string, error) {
	if j.out == "" {
		return nil, usageErr(fs, "-out is required")
	}
	if fs.NArg() == 0 {
		return nil, usageErr(fs, "no input files")
	}
	inputs, err := expandInputs(fs.Args())
	if err != nil {
		return nil, err
	}
	for i, in := range inputs {
		if inputs[i], err = filepath.Abs(in); err != nil {
			return nil, err
		}
	}
	j.out, err = filepath.Abs(j.out)
	return inputs, err
}

func runCoordinator(ctx context.Context, args []string, stderr io.Writer) error {
	fs := newFlagSet("coordinator", "coordinator [flags] INPUT...", stderr)
	var j jobFlags
	var c clusterFlags
	j.register(fs, "output directory, shared with every worker (required)")
	c.register(fs)
	listen := fs.String("listen", ":7070", "TCP address to serve workers on")
	grace := fs.Duration("grace", 5*time.Second, "after the job ends, how long to wait for workers to collect their exit instruction")
	if err := parse(fs, args); err != nil {
		return err
	}
	inputs, err := jobInputs(fs, &j)
	if err != nil {
		return err
	}
	log := newLogger(stderr, c.verbose)

	l, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	coord, err := coordinator.New(c.config(j, inputs, log))
	if err != nil {
		l.Close()
		return err
	}
	log.Info("coordinator listening", "addr", l.Addr().String())
	served := make(chan error, 1)
	go func() { served <- coord.Serve(l) }()

	select {
	case <-coord.Done():
	case err := <-served:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
		log.Info("interrupted; shutting down")
		coord.Shutdown(0)
		return ctx.Err()
	}
	coord.Shutdown(*grace)
	if err := coord.Err(); err != nil {
		return err
	}
	log.Info("stats", "summary", coord.Stats().String())
	return nil
}

func runWorker(ctx context.Context, args []string, stderr io.Writer) error {
	fs := newFlagSet("worker", "worker [flags]", stderr)
	addr := fs.String("coordinator", "localhost:7070", "coordinator TCP address")
	id := fs.String("id", "", "worker name in logs (default hostname-pid-random)")
	timeout := fs.Duration("timeout", worker.DefaultCoordinatorTimeout, "give up after the coordinator is unreachable this long")
	verbose := fs.Bool("v", false, "log every task")
	if err := parse(fs, args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return usageErr(fs, "worker takes no arguments")
	}
	return worker.Run(ctx, worker.Config{
		Coordinator:        *addr,
		ID:                 *id,
		CoordinatorTimeout: *timeout,
		Logger:             newLogger(stderr, *verbose),
	})
}

func runLocal(ctx context.Context, args []string, stderr io.Writer) error {
	fs := newFlagSet("local", "local [flags] INPUT...", stderr)
	var j jobFlags
	var c clusterFlags
	j.register(fs, "output directory (required)")
	c.register(fs)
	workers := fs.Int("workers", runtime.NumCPU(), "number of workers")
	if err := parse(fs, args); err != nil {
		return err
	}
	inputs, err := jobInputs(fs, &j)
	if err != nil {
		return err
	}
	log := newLogger(stderr, c.verbose)
	res, err := cluster.Run(ctx, cluster.Options{Job: c.config(j, inputs, log), Workers: *workers})
	if err != nil {
		return err
	}
	log.Info("stats", "summary", res.Stats.String())
	return nil
}

func runSequential(args []string, stderr io.Writer) error {
	fs := newFlagSet("sequential", "sequential [flags] INPUT...", stderr)
	var j jobFlags
	j.register(fs, "output file (required)")
	if err := parse(fs, args); err != nil {
		return err
	}
	inputs, err := jobInputs(fs, &j)
	if err != nil {
		return err
	}
	jb, err := job.New(j.job, j.params)
	if err != nil {
		return err
	}
	results, err := sequential.Run(jb, inputs)
	if err != nil {
		return err
	}
	return sequential.WriteFile(j.out, results)
}

func listJobs(stdout io.Writer) error {
	tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	for _, d := range job.List() {
		fmt.Fprintf(tw, "%s\t%s\n", d.Name, d.Description)
		for name, help := range d.Params {
			fmt.Fprintf(tw, "\t  -param %s=...  %s\n", name, help)
		}
	}
	return tw.Flush()
}
