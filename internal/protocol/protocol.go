// Package protocol defines the RPC messages exchanged between the coordinator
// and its workers. Calls go over plain TCP using net/rpc with gob encoding.
package protocol

import (
	"fmt"
	"time"
)

// Version is bumped whenever a message changes incompatibly. The coordinator
// rejects workers that speak a different version.
const Version = 1

// ServiceName is the net/rpc service the coordinator registers.
const ServiceName = "Coordinator"

// RPC method names.
const (
	MethodRequestTask = ServiceName + ".RequestTask"
	MethodRenewLease  = ServiceName + ".RenewLease"
	MethodReportTask  = ServiceName + ".ReportTask"
)

// Kind says whether a task is a map or a reduce task.
type Kind int

// Task kinds.
const (
	Map Kind = iota + 1
	Reduce
)

func (k Kind) String() string {
	switch k {
	case Map:
		return "map"
	case Reduce:
		return "reduce"
	}
	return fmt.Sprintf("Kind(%d)", int(k))
}

// Action tells a worker what to do next.
type Action int

const (
	// Run means the reply carries a task to execute.
	Run Action = iota + 1
	// Wait means no task is available right now; ask again.
	Wait
	// Exit means the job is over and the worker should stop.
	Exit
)

// Task is one attempt at a map or reduce task.
type Task struct {
	Kind    Kind
	ID      int
	Attempt int // unique across the job; names this attempt's files

	Job    string
	Params map[string]string

	OutputDir string
	NReduce   int

	// Input is the file a map task reads.
	Input string
	// MapOutputs lists, for a reduce task, the accepted attempt directory of
	// every map task.
	MapOutputs []string

	// LeaseTimeout is how long the attempt stays assigned without a lease
	// renewal before the coordinator gives the task to someone else.
	LeaseTimeout time.Duration
}

// RequestTaskArgs asks the coordinator for work. The coordinator may hold the
// call open for a short while until work becomes available.
type RequestTaskArgs struct {
	Version  int
	WorkerID string
}

// RequestTaskReply carries the next action, and a task when Action is Run.
type RequestTaskReply struct {
	Action Action
	Task   Task
}

// TaskRef identifies one attempt of one task.
type TaskRef struct {
	WorkerID string
	Kind     Kind
	ID       int
	Attempt  int
}

// RenewLeaseArgs extends the lease of a running attempt.
type RenewLeaseArgs struct {
	TaskRef
}

// RenewLeaseReply says whether the attempt is still useful. Continue is false
// once the coordinator no longer tracks the attempt: its lease expired, it
// already reported, another attempt completed the task, or the job is over. A
// worker told not to continue abandons the attempt.
type RenewLeaseReply struct {
	Continue bool
}

// ReportTaskArgs reports how an attempt ended. An empty Error means success:
// for a map task, every partition file exists in the attempt directory; for a
// reduce task, the output file has been committed.
type ReportTaskArgs struct {
	TaskRef
	Error string
}

// ReportTaskReply says whether the report changed the job state. A success
// report for a task that another attempt already completed is not accepted.
type ReportTaskReply struct {
	Accepted bool
}
