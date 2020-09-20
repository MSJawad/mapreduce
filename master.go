package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

//
// Master class
//
type Master struct {
	finishLock sync.Mutex
	condcheck  *sync.Cond

	maptasksDone    int
	mapTaskFinished bool

	reduceTaskDone     int
	reduceTaskFinished bool
}

var maptasks chan string
var reducetasks chan int

var numReduce int

//
// Alternate handler used for now to handle worker connections
//
func (m *Master) Handler(args *MyArgs, reply *MyReply) error {

	msg := args.MessageType
	switch msg {
	case (requestJob):
		select {
		case filename := <-maptasks:

			reply.JobAssigned = true
			reply.JobType = mapJob
			reply.Content = filename
			reply.Nreduce = numReduce

			return nil
		default:
			reply.JobAssigned = false
			if m.mapTaskFinished {
				reply.JobType = finishAllJobs
			} else {
				reply.JobType = noJob
			}
			reply.Content = ""
			return nil
		}
	case (finishedMapJob):
		m.finishLock.Lock()

		m.maptasksDone++
		reply.JobAssigned = false
		reply.JobType = finishedMapJob
		reply.Content = strconv.Itoa(m.maptasksDone)

		m.condcheck.Broadcast()
		m.finishLock.Unlock()

	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := m.mapTaskFinished
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	maptasks = make(chan string)
	reducetasks = make(chan int)
	m := Master{}
	m.condcheck = sync.NewCond(&m.finishLock)
	m.mapTaskFinished = false
	m.maptasksDone = 0
	numReduce = nReduce

	// Your code here.
	go func() {
		for _, file := range files {
			maptasks <- file
		}
	}()

	go func(tasks int) {
		m.finishLock.Lock()
		for m.maptasksDone != tasks {
			m.condcheck.Wait()
		}
		m.mapTaskFinished = true
	}(len(files))

	go func(reduce int) {

		for !m.mapTaskFinished {
			m.condcheck.Wait()
		}
		for i := 0; i < reduce; i++ {
			reducetasks <- i
		}

	}(nReduce)

	m.server()

	return &m
}
