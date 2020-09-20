package mr

import (
	"os"
	"strconv"
)

const (
	requestJob = iota
	mapJob
	reduceJob
	noJob
	finishedMapJob
	finishedReduceJob
	finishAllJobs
)

// arg struct
type MyArgs struct {
	MessageType int
}

// reply struct
type MyReply struct {
	JobAssigned bool
	JobType     int
	Content     string
	Nreduce     int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
